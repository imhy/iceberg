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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestDateToTimestampRewrite {
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
  void stagesPromotedDates(FileFormat format, boolean partitioned) throws Exception {
    startSpark();
    Table table = table(3, format, partitioned);
    String group = "rewrite-dates";
    BatchWrite write =
        new SparkRewriteWriteBuilder(spark, table, table.schema(), group, info()).build().toBatch();
    WriterCommitMessage message;
    try (DataWriter<InternalRow> writer =
        write.createBatchWriterFactory(() -> 1).createWriter(0, 0)) {
      for (Integer days : Arrays.asList(-1, 0, 1, null)) {
        writer.write(new GenericInternalRow(new Object[] {days}));
      }
      message = writer.commit();
    }
    write.commit(new WriterCommitMessage[] {message});
    FileRewriteCoordinator coordinator = FileRewriteCoordinator.get();
    try {
      Set<DataFile> files = coordinator.fetchNewFiles(table, group);
      assertThat(files).isNotEmpty();
      assertThat(table.currentSnapshot()).isNull();
      if (partitioned) {
        assertThat(files)
            .extracting(file -> file.partition().get(0, Integer.class))
            .containsExactlyInAnyOrder(-24, 0, 24, null);
      }
      AppendFiles append = table.newAppend();
      files.forEach(append::appendFile);
      append.commit();
      List<Object> values = Lists.newArrayList();
      try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
        rows.forEach(row -> values.add(row.getField("d")));
      }
      assertThat(values)
          .containsExactlyInAnyOrder(
              LocalDate.ofEpochDay(-1).atStartOfDay(),
              LocalDate.ofEpochDay(0).atStartOfDay(),
              LocalDate.ofEpochDay(1).atStartOfDay(),
              null);
    } finally {
      coordinator.clearRewrite(table, group);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void rejectsDateInputForOlderTableVersions(int version) {
    startSpark();
    Table table = table(version, FileFormat.PARQUET, false);
    assertThatThrownBy(
            () ->
                new SparkRewriteWriteBuilder(spark, table, table.schema(), "rewrite-dates", info())
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("date cannot be promoted to timestamp");
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

  private static LogicalWriteInfo info() {
    LogicalWriteInfo info = mock(LogicalWriteInfo.class);
    when(info.queryId()).thenReturn("rewrite-dates");
    when(info.schema()).thenReturn(SparkSchemaUtil.convert(schema(Types.DateType.get())));
    when(info.options()).thenReturn(new CaseInsensitiveStringMap(Map.of()));
    return info;
  }
}
