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
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestDateToTimestampPlanning {
  private static final Schema DATE_SCHEMA = schema(Types.DateType.get());
  @TempDir private File temp;

  static Stream<Arguments> readModes() {
    return Stream.of(Types.TimestampType.withoutZone(), Types.TimestampNanoType.withoutZone())
        .flatMap(type -> Stream.of(Arguments.of(type, true), Arguments.of(type, false)));
  }

  @AfterEach
  void cleanup() {
    TestTables.clearTables();
  }

  @ParameterizedTest
  @MethodSource("readModes")
  void readsHistoricalDateAsTimestamp(Type.PrimitiveType type, boolean dictionary)
      throws IOException {
    DataFile file =
        write(
            "old",
            DATE_SCHEMA,
            dictionary,
            LocalDate.ofEpochDay(-1),
            LocalDate.ofEpochDay(0),
            LocalDate.ofEpochDay(1),
            null);
    try (CloseableIterable<Record> rows =
        Parquet.read(Files.localInput(file.location()))
            .project(schema(type))
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(schema(type), fileSchema))
            .build()) {
      assertThat(rows)
          .extracting(row -> row.getField("d"))
          .containsExactly(
              LocalDate.ofEpochDay(-1).atStartOfDay(),
              LocalDate.ofEpochDay(0).atStartOfDay(),
              LocalDate.ofEpochDay(1).atStartOfDay(),
              null);
    }
  }

  @ParameterizedTest
  @MethodSource("readModes")
  void filteredReadsMixedFiles(Type.PrimitiveType type, boolean dictionary) throws IOException {
    Table table =
        TestTables.create(
            new File(temp, "table"), "test", DATE_SCHEMA, PartitionSpec.unpartitioned(), 3);
    DataFile old =
        write(
            "old",
            DATE_SCHEMA,
            dictionary,
            LocalDate.ofEpochDay(-1),
            LocalDate.ofEpochDay(0),
            LocalDate.ofEpochDay(1),
            null);
    DataFile disjoint = write("disjoint", DATE_SCHEMA, dictionary, LocalDate.ofEpochDay(10));
    table.newAppend().appendFile(old).appendFile(disjoint).commit();
    evolve(table, type);
    LocalDateTime midnight = LocalDate.ofEpochDay(1).atStartOfDay();
    LocalDateTime afternoon = midnight.plusHours(12);
    DataFile current = write("new", schema(type), dictionary, midnight, afternoon, null);
    table.newAppend().appendFile(current).commit();
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().filter(Expressions.equal("d", midnight.toString())).planFiles()) {
      assertThat(tasks)
          .extracting(task -> task.file().location())
          .containsExactlyInAnyOrder(old.location(), current.location());
    }
    assertRows(table, Expressions.equal("d", midnight.toString()), midnight, midnight);
    assertRows(
        table,
        Expressions.in("d", midnight.toString(), afternoon.toString()),
        midnight,
        midnight,
        afternoon);
    assertRows(
        table,
        Expressions.and(
            Expressions.greaterThanOrEqual("d", midnight.toString()),
            Expressions.lessThan("d", midnight.plusDays(1).toString())),
        midnight,
        midnight,
        afternoon);
    assertRows(
        table,
        Expressions.and(Expressions.notNull("d"), Expressions.lessThan("d", "1970-01-01T00:00:00")),
        LocalDate.ofEpochDay(-1).atStartOfDay());
    assertRows(table, Expressions.equal("d", afternoon.toString()), afternoon);
    assertRows(table, Expressions.isNull("d"), null, null);
    assertRows(table, Expressions.equal("d", midnight.plusDays(2).toString()));
  }

  @ParameterizedTest
  @MethodSource("readModes")
  void readsFileWithConservativeOutOfRangeBounds(Type.PrimitiveType type, boolean dictionary)
      throws IOException {
    Table table =
        TestTables.create(
            new File(temp, "table"), "test", DATE_SCHEMA, PartitionSpec.unpartitioned(), 3);
    long unitsPerDay =
        type.typeId() == Type.TypeID.TIMESTAMP ? 86_400_000_000L : 86_400_000_000_000L;
    int day = (int) (Long.MAX_VALUE / unitsPerDay) + 1;
    DataFile written =
        write(
            "conservative",
            DATE_SCHEMA,
            dictionary,
            LocalDate.ofEpochDay(0),
            LocalDate.ofEpochDay(1),
            LocalDate.ofEpochDay(1));
    // The values are days 0, 1, and 1, but the persisted bounds conservatively enclose them with
    // dates that overflow the target timestamp type, which the spec allows.
    DataFile conservative =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .copy(written)
            .withMetrics(
                new Metrics(
                    3L,
                    null,
                    Map.of(1, 3L),
                    Map.of(1, 0L),
                    null,
                    Map.of(1, Conversions.toByteBuffer(Types.DateType.get(), -day)),
                    Map.of(1, Conversions.toByteBuffer(Types.DateType.get(), day))))
            .build();
    table.newAppend().appendFile(conservative).commit();
    evolve(table, type);
    LocalDateTime epoch = LocalDate.ofEpochDay(0).atStartOfDay();
    LocalDateTime midnight = LocalDate.ofEpochDay(1).atStartOfDay();
    assertRows(table, Expressions.alwaysTrue(), epoch, midnight, midnight);
    assertRows(table, Expressions.equal("d", "1970-01-02T00:00:00"), midnight, midnight);
  }

  @ParameterizedTest
  @MethodSource("readModes")
  void overflowingDateFailsReading(Type.PrimitiveType type, boolean dictionary) throws IOException {
    long unitsPerDay =
        type.typeId() == Type.TypeID.TIMESTAMP ? 86_400_000_000L : 86_400_000_000_000L;
    DataFile file =
        write(
            "overflow",
            DATE_SCHEMA,
            dictionary,
            LocalDate.ofEpochDay(Long.MAX_VALUE / unitsPerDay + 1));
    assertThatThrownBy(
            () -> {
              try (CloseableIterable<Record> rows =
                  Parquet.read(Files.localInput(file.location()))
                      .project(schema(type))
                      .createReaderFunc(
                          fileSchema -> GenericParquetReaders.buildReader(schema(type), fileSchema))
                      .build()) {
                rows.iterator().next();
              }
            })
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("overflow");
  }

  private DataFile write(String name, Schema schema, boolean dictionary, Object... values)
      throws IOException {
    File path = new File(temp, name + ".parquet");
    FileAppender<Record> appender =
        Parquet.write(Files.localOutput(path))
            .schema(schema)
            .set("parquet.enable.dictionary", Boolean.toString(dictionary))
            .set(TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "d", "true")
            .createWriterFunc(GenericParquetWriter::create)
            .build();
    try (appender) {
      for (Object value : values) {
        GenericRecord record = GenericRecord.create(schema);
        record.setField("d", value);
        appender.add(record);
      }
    }
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(path.toString())
        .withFileSizeInBytes(appender.length())
        .withMetrics(appender.metrics())
        .build();
  }

  private static Schema schema(Type.PrimitiveType type) {
    return new Schema(Types.NestedField.optional(1, "d", type));
  }

  private static void evolve(Table table, Type.PrimitiveType type) {
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata base = ops.current();
    ops.commit(
        base,
        TableMetadata.buildFrom(base).setCurrentSchema(schema(type), base.lastColumnId()).build());
    table.refresh();
  }

  private static void assertRows(Table table, Expression filter, Object... expected)
      throws IOException {
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).where(filter).build()) {
      assertThat(rows).extracting(row -> row.getField("d")).containsExactlyInAnyOrder(expected);
    }
  }
}
