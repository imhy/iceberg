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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.DeltaBatchWrite;
import org.apache.spark.sql.connector.write.DeltaWriter;
import org.apache.spark.sql.connector.write.DeltaWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation.Command;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestDateToTimestampDeltaWrite {
  @TempDir Path temp;
  private SparkSession spark;

  static Stream<Arguments> modes() {
    return Stream.of(FileFormat.PARQUET, FileFormat.AVRO, FileFormat.ORC)
        .flatMap(
            format -> Stream.of(false, true).map(partitioned -> Arguments.of(format, partitioned)));
  }

  @AfterEach
  void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  private void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config(TestBase.DISABLE_UI)
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate();
  }

  @ParameterizedTest
  @MethodSource("modes")
  void commitsPromotedDates(FileFormat format, boolean partitioned) throws Exception {
    startSpark();
    Table table = table(3, format, partitioned);
    table.replaceSortOrder().asc("d").commit();
    SparkPositionDeltaWrite delta =
        (SparkPositionDeltaWrite)
            new SparkPositionDeltaWriteBuilder(
                    spark,
                    table,
                    null,
                    Command.MERGE,
                    null,
                    IsolationLevel.SERIALIZABLE,
                    info(table))
                .build();
    assertThat(delta.requiredOrdering())
        .extracting(order -> order.expression().describe())
        .contains("date_to_timestamp_ntz(d)");
    if (partitioned) {
      assertThat(delta.requiredDistribution().toString())
          .contains("hours(date_to_timestamp_ntz(d))");
    }
    DeltaBatchWrite write = delta.toBatch();
    WriterCommitMessage message;
    try (DeltaWriter<InternalRow> writer = roundTrip(write).createWriter(0, 0)) {
      for (Integer days : Arrays.asList(null, -1, 0, 1)) {
        writer.insert(new GenericInternalRow(new Object[] {days}));
      }
      message = writer.commit();
    }
    SparkPositionDeltaWrite.DeltaTaskCommit result =
        (SparkPositionDeltaWrite.DeltaTaskCommit) message;
    assertThat(result.dataFiles()).isNotEmpty();
    if (partitioned) {
      assertThat(result.dataFiles())
          .extracting(file -> file.partition().get(0, Integer.class))
          .containsExactlyInAnyOrder(-24, 0, 24, null);
    }
    write.commit(new WriterCommitMessage[] {message});
    List<Object> values = new ArrayList<>();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      rows.forEach(row -> values.add(row.getField("d")));
    }
    assertThat(values)
        .containsExactlyInAnyOrder(
            LocalDate.ofEpochDay(-1).atStartOfDay(),
            LocalDate.ofEpochDay(0).atStartOfDay(),
            LocalDate.ofEpochDay(1).atStartOfDay(),
            null);
    // A delete-only write has no data schema. Its file/position/partition metadata stays unchanged.
    DataFile file = result.dataFiles()[0];
    Object removed;
    try (CloseableIterable<Record> rows =
        FormatModelRegistry.readBuilder(file.format(), Record.class, table.io().newInputFile(file))
            .project(table.schema())
            .build()) {
      removed = rows.iterator().next().getField("d");
    }
    LogicalWriteInfo deleteInfo = info(table);
    when(deleteInfo.schema()).thenReturn(new StructType());
    DeltaBatchWrite deletes =
        new SparkPositionDeltaWriteBuilder(
                spark, table, null, Command.DELETE, null, IsolationLevel.SERIALIZABLE, deleteInfo)
            .build()
            .toBatch();
    Object[] partition = new Object[file.partition().size()];
    for (int i = 0; i < partition.length; i++) {
      partition[i] = file.partition().get(i, Object.class);
    }
    WriterCommitMessage deleted;
    try (DeltaWriter<InternalRow> writer = roundTrip(deletes).createWriter(0, 1)) {
      writer.delete(
          new GenericInternalRow(new Object[] {file.specId(), new GenericInternalRow(partition)}),
          new GenericInternalRow(new Object[] {UTF8String.fromString(file.location()), 0L}));
      deleted = writer.commit();
    }
    deletes.commit(new WriterCommitMessage[] {deleted});
    values.remove(removed);
    List<Object> remaining = new ArrayList<>();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      rows.forEach(row -> remaining.add(row.getField("d")));
    }
    assertThat(remaining).containsExactlyInAnyOrderElementsOf(values);
  }

  @ParameterizedTest
  @ValueSource(ints = {2})
  void rejectsDateInputForOlderTableVersions(int version) {
    startSpark();
    Table table = table(version, FileFormat.PARQUET, false);
    assertThatThrownBy(
            () ->
                new SparkPositionDeltaWriteBuilder(
                        spark,
                        table,
                        null,
                        Command.MERGE,
                        null,
                        IsolationLevel.SERIALIZABLE,
                        info(table))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("date cannot be promoted to timestamp");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectsOverflowBeforeCommitting(boolean partitioned) throws Exception {
    startSpark();
    Table table = table(3, FileFormat.PARQUET, partitioned);
    DeltaBatchWrite write =
        new SparkPositionDeltaWriteBuilder(
                spark, table, null, Command.MERGE, null, IsolationLevel.SERIALIZABLE, info(table))
            .build()
            .toBatch();
    try (DeltaWriter<InternalRow> writer = roundTrip(write).createWriter(0, 0)) {
      for (int days :
          new int[] {
            (int) (Long.MIN_VALUE / 86_400_000_000L) - 1,
            (int) (Long.MAX_VALUE / 86_400_000_000L) + 1
          }) {
        assertThatThrownBy(() -> writer.insert(new GenericInternalRow(new Object[] {days})))
            .isInstanceOf(ArithmeticException.class);
      }
      writer.abort();
    }
    assertThat(table.currentSnapshot()).isNull();
  }

  @ParameterizedTest
  @MethodSource("modes")
  void preservesV2TimestampWrites(FileFormat format, boolean partitioned) throws Exception {
    startSpark();
    Table table = table(2, format, partitioned);
    LogicalWriteInfo info = info(table);
    when(info.schema()).thenReturn(SparkSchemaUtil.convert(table.schema()));
    DeltaBatchWrite write =
        new SparkPositionDeltaWriteBuilder(
                spark, table, null, Command.MERGE, null, IsolationLevel.SERIALIZABLE, info)
            .build()
            .toBatch();
    WriterCommitMessage message;
    try (DeltaWriter<InternalRow> writer = roundTrip(write).createWriter(0, 0)) {
      writer.insert(new GenericInternalRow(new Object[] {86_400_000_000L}));
      message = writer.commit();
    }
    write.commit(new WriterCommitMessage[] {message});
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      assertThat(rows)
          .singleElement()
          .satisfies(
              row ->
                  assertThat(row.getField("d")).isEqualTo(LocalDate.ofEpochDay(1).atStartOfDay()));
    }
  }

  private static DeltaWriterFactory roundTrip(DeltaBatchWrite write) {
    return SerializationUtil.deserializeFromBytes(
        SerializationUtil.serializeToBytes(write.createBatchWriterFactory(() -> 1)));
  }

  private Table table(int version, FileFormat format, boolean partitioned) {
    Schema schema = schema(Types.TimestampType.withoutZone());
    return new HadoopTables(new Configuration())
        .create(
            schema,
            partitioned
                ? PartitionSpec.builderFor(schema).hour("d").build()
                : PartitionSpec.unpartitioned(),
            Map.of(
                "format-version", Integer.toString(version), "write.format.default", format.name()),
            temp.resolve("table").toString());
  }

  private static Schema schema(Type type) {
    return new Schema(Types.NestedField.optional(1, "d", type));
  }

  private static LogicalWriteInfo info(Table table) {
    LogicalWriteInfo info = mock(LogicalWriteInfo.class);
    when(info.queryId()).thenReturn("rewrite-dates");
    when(info.schema()).thenReturn(SparkSchemaUtil.convert(schema(Types.DateType.get())));
    when(info.options()).thenReturn(new CaseInsensitiveStringMap(Map.of()));
    when(info.rowIdSchema())
        .thenReturn(
            Optional.of(
                SparkSchemaUtil.convert(
                    new Schema(MetadataColumns.FILE_PATH, MetadataColumns.ROW_POSITION))));
    when(info.metadataSchema())
        .thenReturn(
            Optional.of(
                SparkSchemaUtil.convert(
                    new Schema(
                        MetadataColumns.SPEC_ID,
                        MetadataColumns.metadataColumn(
                            table, MetadataColumns.PARTITION_COLUMN_NAME)))));
    return info;
  }
}
