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
package org.apache.iceberg.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestDateToTimestampEvolution {
  private static final Schema DATE_SCHEMA =
      new Schema(Types.NestedField.optional(1, "d", Types.DateType.get()));
  @TempDir private File temp;

  static Stream<Type.PrimitiveType> targets() {
    return Stream.of(Types.TimestampType.withoutZone(), Types.TimestampNanoType.withoutZone());
  }

  static Stream<Arguments> readModes() {
    return targets()
        .flatMap(
            target ->
                Stream.of(true, false)
                    .flatMap(
                        partitioned ->
                            Stream.of(
                                Arguments.of(target, FileFormat.PARQUET, true, partitioned),
                                Arguments.of(target, FileFormat.PARQUET, false, partitioned),
                                Arguments.of(target, FileFormat.ORC, false, partitioned))));
  }

  @AfterEach
  void cleanup() {
    TestTables.clearTables();
  }

  @ParameterizedTest
  @MethodSource("readModes")
  void publicEvolutionReadsMixedFiles(
      Type.PrimitiveType target, FileFormat format, boolean dictionary, boolean partitioned)
      throws IOException {
    PartitionSpec spec =
        partitioned
            ? PartitionSpec.builderFor(DATE_SCHEMA).day("d").build()
            : PartitionSpec.unpartitioned();
    Table table = TestTables.create(new File(temp, "table"), "test", DATE_SCHEMA, spec, 3);
    DataFile negative = write(table, format, "negative", dictionary, -1, LocalDate.ofEpochDay(-1));
    DataFile zero = write(table, format, "zero", dictionary, 0, LocalDate.ofEpochDay(0));
    DataFile old = write(table, format, "old", dictionary, 1, LocalDate.ofEpochDay(1));
    DataFile disjoint = write(table, format, "disjoint", dictionary, 10, LocalDate.ofEpochDay(10));
    DataFile oldNull = write(table, format, "old-null", dictionary, null, (Object) null);
    table
        .newAppend()
        .appendFile(negative)
        .appendFile(zero)
        .appendFile(old)
        .appendFile(disjoint)
        .appendFile(oldNull)
        .commit();
    TableMetadata original = metadata(table);
    List<String> manifests =
        original.currentSnapshot().allManifests(table.io()).stream().map(m -> m.path()).toList();
    table.updateSchema().updateColumn("d", target).commit();
    reload(table);
    assertThat(table.currentSnapshot().snapshotId())
        .isEqualTo(original.currentSnapshot().snapshotId());
    assertThat(table.currentSnapshot().allManifests(table.io()))
        .extracting(m -> m.path())
        .containsExactlyElementsOf(manifests);
    assertThat(table.schemas().get(original.currentSchemaId()).asStruct())
        .isEqualTo(original.schema().asStruct());
    try (CloseableIterable<FileScanTask> tasks = table.newScan().includeColumnStats().planFiles()) {
      assertThat(tasks)
          .allSatisfy(
              task -> {
                if (!task.file().location().equals(oldNull.location())) {
                  assertThat(task.file().lowerBounds().get(1).remaining()).isEqualTo(Integer.BYTES);
                }
              });
    }
    LocalDateTime midnight = LocalDate.ofEpochDay(1).atStartOfDay();
    LocalDateTime afternoon = midnight.plusHours(12);
    DataFile current = write(table, format, "current", dictionary, 1, midnight, afternoon);
    DataFile newNull = write(table, format, "new-null", dictionary, null, (Object) null);
    table.newAppend().appendFile(current).appendFile(newNull).commit();
    Expression equality = Expressions.equal("d", midnight.toString());
    try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(equality).planFiles()) {
      assertThat(tasks)
          .extracting(t -> t.file().location())
          .containsExactlyInAnyOrder(old.location(), current.location());
    }
    assertRows(table, equality, midnight, midnight);
    assertRows(
        table,
        Expressions.in("d", "1969-12-31T00:00:00", afternoon.toString()),
        LocalDate.ofEpochDay(-1).atStartOfDay(),
        afternoon);
    assertRows(
        table,
        Expressions.and(
            Expressions.greaterThanOrEqual("d", "1970-01-01T00:00:00"),
            Expressions.lessThan("d", "1970-01-03T00:00:00")),
        LocalDate.ofEpochDay(0).atStartOfDay(),
        midnight,
        midnight,
        afternoon);
    assertRows(table, Expressions.isNull("d"), null, null);
    assertRows(table, Expressions.equal("d", "1970-01-04T00:00:00"));
  }

  @ParameterizedTest
  @MethodSource("targets")
  void readsOrcPredicatesWithoutFileMetrics(Type.PrimitiveType target) throws IOException {
    Table table =
        TestTables.create(
            new File(temp, "table"), "test", DATE_SCHEMA, PartitionSpec.unpartitioned(), 3);
    DataFile physical =
        write(
            table,
            FileFormat.ORC,
            "dates",
            false,
            null,
            LocalDate.ofEpochDay(-1),
            LocalDate.ofEpochDay(0),
            LocalDate.ofEpochDay(1),
            null);
    table
        .newAppend()
        .appendFile(
            DataFiles.builder(table.spec())
                .copy(physical)
                .withMetrics(new Metrics(physical.recordCount()))
                .build())
        .commit();
    table.updateSchema().updateColumn("d", target).commit();
    reload(table);
    LocalDateTime before = LocalDate.ofEpochDay(-1).atStartOfDay();
    LocalDateTime epoch = LocalDate.ofEpochDay(0).atStartOfDay();
    LocalDateTime after = LocalDate.ofEpochDay(1).atStartOfDay();
    assertRows(table, Expressions.equal("d", epoch.toString()), epoch);
    // Iceberg's evaluator orders null before non-null values for comparisons.
    assertRows(
        table, Expressions.lessThan("d", epoch.plusHours(12).toString()), before, epoch, null);
    assertRows(table, Expressions.greaterThan("d", epoch.plusHours(12).toString()), after);
    assertRows(table, Expressions.in("d", before.toString(), after.toString()), before, after);
    assertRows(table, Expressions.notIn("d", before.toString(), after.toString()), epoch, null);
    assertRows(
        table, Expressions.not(Expressions.equal("d", epoch.toString())), before, after, null);
    assertRows(
        table,
        Expressions.or(Expressions.isNull("d"), Expressions.equal("d", epoch.toString())),
        epoch,
        null);
    assertRows(
        table,
        Expressions.and(Expressions.notNull("d"), Expressions.notEqual("d", epoch.toString())),
        before,
        after);
    assertRows(table, Expressions.isNull("d"), (Object) null);
  }

  @ParameterizedTest
  @MethodSource("targets")
  void convertedInitialDefaultReadsFilesMissingTheField(Type.PrimitiveType target)
      throws IOException {
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));
    Table table =
        TestTables.create(
            new File(temp, "table"), "test", schema, PartitionSpec.unpartitioned(), 3);
    table.newAppend().appendFile(write(table, "missing", false, null, 42L)).commit();
    table
        .updateSchema()
        .addColumn(
            "d", Types.DateType.get(), "defaulted date", Literal.of(1).to(Types.DateType.get()))
        .commit();
    table.updateSchema().updateColumn("d", target).commit();
    reload(table);
    LocalDateTime midnight = LocalDate.ofEpochDay(1).atStartOfDay();
    assertRows(table, Expressions.equal("d", midnight.toString()), midnight);
  }

  @ParameterizedTest
  @MethodSource("targets")
  void overflowIsDeferredUntilPlanning(Type.PrimitiveType target) throws IOException {
    Table table =
        TestTables.create(
            new File(temp, "table"), "test", DATE_SCHEMA, PartitionSpec.unpartitioned(), 3);
    long units = target.typeId() == Type.TypeID.TIMESTAMP ? 86_400_000_000L : 86_400_000_000_000L;
    table
        .newAppend()
        .appendFile(
            write(table, "overflow", false, null, LocalDate.ofEpochDay(Long.MAX_VALUE / units + 1)))
        .commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    table.updateSchema().updateColumn("d", target).commit();
    assertThat(table.schema().findType("d")).isEqualTo(target);
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(snapshotId);
    assertThatThrownBy(
            () -> {
              try (CloseableIterable<FileScanTask> tasks =
                  table
                      .newScan()
                      .filter(Expressions.equal("d", "1970-01-02T00:00:00"))
                      .planFiles()) {
                tasks.iterator().hasNext();
              }
            })
        .isInstanceOf(ArithmeticException.class);
  }

  private DataFile write(
      Table table, String name, boolean dictionary, Integer day, Object... values)
      throws IOException {
    return write(table, FileFormat.PARQUET, name, dictionary, day, values);
  }

  private DataFile write(
      Table table,
      FileFormat format,
      String name,
      boolean dictionary,
      Integer day,
      Object... values)
      throws IOException {
    File path = new File(temp, name + "." + format.name().toLowerCase(Locale.ROOT));
    FileAppender<Record> appender =
        format == FileFormat.ORC
            ? ORC.write(Files.localOutput(path))
                .schema(table.schema())
                .createWriterFunc(GenericOrcWriter::buildWriter)
                .build()
            : Parquet.write(Files.localOutput(path))
                .schema(table.schema())
                .set("parquet.enable.dictionary", Boolean.toString(dictionary))
                .createWriterFunc(GenericParquetWriter::create)
                .build();
    try (appender) {
      for (Object value : values) {
        GenericRecord record = GenericRecord.create(table.schema());
        record.set(0, value);
        appender.add(record);
      }
    }
    PartitionData partition = new PartitionData(table.spec().partitionType());
    if (!table.spec().isUnpartitioned()) {
      partition.set(0, day);
    }
    return DataFiles.builder(table.spec())
        .withPath(path.toString())
        .withFileSizeInBytes(appender.length())
        .withMetrics(appender.metrics())
        .withPartition(partition)
        .build();
  }

  private static TableMetadata metadata(Table table) {
    return ((BaseTable) table).operations().current();
  }

  private static void reload(Table table) {
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata before = ops.current();
    ops.commit(before, TableMetadataParser.fromJson(TableMetadataParser.toJson(before)));
    table.refresh();
  }

  private static void assertRows(Table table, Expression filter, Object... expected)
      throws IOException {
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).where(filter).build()) {
      assertThat(rows).extracting(row -> row.getField("d")).containsExactlyInAnyOrder(expected);
    }
  }
}
