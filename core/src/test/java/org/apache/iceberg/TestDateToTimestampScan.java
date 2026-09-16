/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestDateToTimestampScan {
  private static final Schema DATE_SCHEMA =
      new Schema(Types.NestedField.optional(1, "d", Types.DateType.get()));
  @TempDir private File temp;

  static Stream<Type.PrimitiveType> timestampTypes() {
    return Stream.of(Types.TimestampType.withoutZone(), Types.TimestampNanoType.withoutZone());
  }

  @AfterEach
  void cleanup() {
    TestTables.clearTables();
  }

  @ParameterizedTest
  @MethodSource("timestampTypes")
  void plansHistoricalAndCurrentFiles(Type.PrimitiveType type) throws IOException {
    Table table = TestTables.create(temp, "test", DATE_SCHEMA, PartitionSpec.unpartitioned(), 3);
    for (int day : new int[] {-1, 0, 1}) {
      table.newAppend().appendFile(file("old-" + day, Types.DateType.get(), day)).commit();
    }
    evolve(table, type);
    table.newAppend().appendFile(file("new", type, unitsPerDay(type))).commit();
    TableScan scan = table.newScan();
    for (int day : new int[] {-1, 0, 1}) {
      String midnight = LocalDate.ofEpochDay(day).atStartOfDay().toString();
      String[] matching =
          day == 1
              ? new String[] {"old-1.parquet", "new.parquet"}
              : new String[] {"old-" + day + ".parquet"};
      assertPaths(scan.filter(Expressions.equal("d", midnight)), matching);
      assertPaths(scan.filter(Expressions.in("d", midnight)), matching);
    }
    assertPaths(
        scan.filter(Expressions.greaterThanOrEqual("d", "1970-01-02T00:00:00")),
        "old-1.parquet",
        "new.parquet");
    assertPaths(scan.filter(Expressions.lessThan("d", "1970-01-01T00:00:00")), "old--1.parquet");
    assertPaths(scan, "old--1.parquet", "old-0.parquet", "old-1.parquet", "new.parquet");
    try (CloseableIterable<FileScanTask> tasks = scan.includeColumnStats().planFiles()) {
      for (FileScanTask task : tasks) {
        int size = task.file().location().startsWith("old") ? Integer.BYTES : Long.BYTES;
        assertThat(task.file().lowerBounds().get(1).remaining()).isEqualTo(size);
        assertThat(task.file().upperBounds().get(1).remaining()).isEqualTo(size);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("timestampTypes")
  void overflowingBoundFailsPlanning(Type.PrimitiveType type) {
    Table table = TestTables.create(temp, "test", DATE_SCHEMA, PartitionSpec.unpartitioned(), 3);
    int day = (int) (Long.MAX_VALUE / unitsPerDay(type)) + 1;
    table.newAppend().appendFile(file("overflow", Types.DateType.get(), day)).commit();
    evolve(table, type);
    assertThatThrownBy(
            () ->
                assertPaths(table.newScan().filter(Expressions.equal("d", "1970-01-02T00:00:00"))))
        .isInstanceOf(ArithmeticException.class);
  }

  @ParameterizedTest
  @MethodSource("timestampTypes")
  void plansDayPartitionedFiles(Type.PrimitiveType type) throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(DATE_SCHEMA).day("d").build();
    Table table = TestTables.create(temp, "test", DATE_SCHEMA, spec, 3);
    for (int day : new int[] {-1, 1, 2}) {
      DataFile old = file("old-" + day, Types.DateType.get(), day);
      table
          .newAppend()
          .appendFile(
              DataFiles.builder(spec).copy(old).withPartition(TestHelpers.Row.of(day)).build())
          .commit();
    }
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata base = ops.current();
    TableMetadata withSchema =
        TableMetadata.buildFrom(base)
            .addSchema(new Schema(Types.NestedField.optional(1, "d", type)))
            .build();
    // Load an externally evolved history so persisted day transforms bind to the current type.
    TableMetadata evolved =
        JsonUtil.parse(
            TableMetadataParser.toJson(withSchema),
            node -> {
              ((ObjectNode) node)
                  .put(
                      "current-schema-id",
                      withSchema.schemas().get(withSchema.schemas().size() - 1).schemaId());
              return TableMetadataParser.fromJson(node);
            });
    ops.commit(base, evolved);
    table.refresh();
    assertThat(table.spec().partitionType()).isEqualTo(spec.partitionType());
    long timestamp = unitsPerDay(type);
    Object datePartition = Transforms.<Integer>day().bind(Types.DateType.get()).apply(1);
    Object timestampPartition = Transforms.<Long>day().bind(type).apply(timestamp);
    assertThat(timestampPartition).isEqualTo(datePartition);
    table
        .newAppend()
        .appendFile(
            DataFiles.builder(table.spec())
                .copy(file("new", type, timestamp))
                .withPartition(TestHelpers.Row.of(1))
                .build())
        .commit();
    assertPaths(
        table.newScan().filter(Expressions.equal("d", "1970-01-02T00:00:00")),
        "old-1.parquet",
        "new.parquet");
    assertPaths(
        table.newScan().filter(Expressions.lessThan("d", "1970-01-01T00:00:00")), "old--1.parquet");
  }

  @ParameterizedTest
  @MethodSource("timestampTypes")
  void plansFilesWithoutBounds(Type.PrimitiveType type) throws IOException {
    Table table = TestTables.create(temp, "test", DATE_SCHEMA, PartitionSpec.unpartitioned(), 3);
    DataFile missing =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("missing.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(1)
            .build();
    DataFile nulls =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("nulls.parquet")
            .withFileSizeInBytes(100)
            .withMetrics(new Metrics(1L, null, Map.of(1, 1L), Map.of(1, 1L), null, null, null))
            .build();
    table.newAppend().appendFile(missing).appendFile(nulls).commit();
    evolve(table, type);
    assertPaths(
        table.newScan().filter(Expressions.equal("d", "1970-01-02T00:00:00")), "missing.parquet");
    assertPaths(
        table.newScan().filter(Expressions.isNull("d")), "missing.parquet", "nulls.parquet");
  }

  @ParameterizedTest
  @MethodSource("timestampTypes")
  void associatesCurrentDeletesWithHistoricalData(Type.PrimitiveType type) throws IOException {
    Table table = TestTables.create(temp, "test", DATE_SCHEMA, PartitionSpec.unpartitioned(), 3);
    table
        .newAppend()
        .appendFile(file("matching", Types.DateType.get(), 1))
        .appendFile(file("disjoint", Types.DateType.get(), 3))
        .commit();
    evolve(table, type);
    ByteBuffer bound = Conversions.toByteBuffer(type, unitsPerDay(type));
    DeleteFile delete =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofEqualityDeletes(1)
            .withPath("delete.parquet")
            .withFileSizeInBytes(100)
            .withMetrics(
                new Metrics(
                    1L,
                    null,
                    Map.of(1, 1L),
                    Map.of(1, 0L),
                    null,
                    Map.of(1, bound),
                    Map.of(1, bound)))
            .build();
    table.newRowDelta().addDeletes(delete).commit();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        if (task.file().location().equals("matching.parquet")) {
          assertThat(task.deletes())
              .extracting(DeleteFile::location)
              .containsExactly("delete.parquet");
        } else {
          assertThat(task.file().location()).isEqualTo("disjoint.parquet");
          assertThat(task.deletes()).isEmpty();
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("timestampTypes")
  void associatesHistoricalDeletesAfterEvolution(Type.PrimitiveType type) throws IOException {
    Table table = TestTables.create(temp, "test", DATE_SCHEMA, PartitionSpec.unpartitioned(), 3);
    table
        .newAppend()
        .appendFile(file("matching", Types.DateType.get(), -1))
        .appendFile(file("disjoint", Types.DateType.get(), 3))
        .commit();
    ByteBuffer bound = Conversions.toByteBuffer(Types.DateType.get(), -1);
    DeleteFile delete =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofEqualityDeletes(1)
            .withPath("delete.parquet")
            .withFileSizeInBytes(100)
            .withMetrics(
                new Metrics(
                    1L,
                    null,
                    Map.of(1, 1L),
                    Map.of(1, 0L),
                    null,
                    Map.of(1, bound),
                    Map.of(1, bound)))
            .build();
    table.newRowDelta().addDeletes(delete).commit();
    evolve(table, type);
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        assertThat(task.deletes())
            .hasSize(task.file().location().equals("matching.parquet") ? 1 : 0);
      }
    }
  }

  private static long unitsPerDay(Type type) {
    return type.typeId() == Type.TypeID.TIMESTAMP ? 86_400_000_000L : 86_400_000_000_000L;
  }

  private static DataFile file(String name, Type type, Object value) {
    ByteBuffer bound = Conversions.toByteBuffer(type, value);
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(name + ".parquet")
        .withFileSizeInBytes(100)
        .withMetrics(
            new Metrics(
                1L, null, Map.of(1, 1L), Map.of(1, 0L), null, Map.of(1, bound), Map.of(1, bound)))
        .build();
  }

  private static void evolve(Table table, Type.PrimitiveType type) {
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata base = ops.current();
    Schema evolved = new Schema(Types.NestedField.optional(1, "d", type));
    // Construct the valid v3 history independently of public schema-update support.
    ops.commit(
        base, TableMetadata.buildFrom(base).setCurrentSchema(evolved, base.lastColumnId()).build());
    table.refresh();
    assertThat(table.schemas().values())
        .anyMatch(schema -> schema.findType(1).equals(Types.DateType.get()));
    assertThat(table.schema().findType(1)).isEqualTo(type);
  }

  private static void assertPaths(TableScan scan, String... paths) throws IOException {
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      assertThat(tasks).extracting(task -> task.file().location()).containsExactlyInAnyOrder(paths);
    }
  }
}
