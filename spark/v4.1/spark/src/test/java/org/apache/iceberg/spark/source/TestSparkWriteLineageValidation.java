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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.ProjectingInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.expressions.filter.AlwaysTrue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation.Command;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.collection.JavaConverters;

class TestSparkWriteLineageValidation {
  private static SparkSession spark;
  @TempDir Path temp;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config(TestBase.DISABLE_UI)
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    spark.stop();
  }

  @ParameterizedTest
  @ValueSource(strings = {"append", "dynamic", "overwrite"})
  void rejectsLineageOutsideCopyOnWrite(String mode) {
    SparkWriteBuilder builder = builder(table(), lineage());
    switch (mode) {
      case "dynamic" -> builder.overwriteDynamicPartitions();
      case "overwrite" -> builder.overwrite(new Predicate[] {new AlwaysTrue()});
      default -> {}
    }
    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Row lineage metadata is not supported");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void rejectsIncompleteLineage(boolean rowId) {
    Schema metadata =
        new Schema(rowId ? MetadataColumns.ROW_ID : MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);
    SparkWriteBuilder builder = builder(table(), SparkSchemaUtil.convert(metadata));
    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Row lineage metadata must contain both");
  }

  @Test
  void rejectsMissingCopyOnWriteLineage() {
    SparkWriteBuilder builder = builder(table(), new StructType());
    builder.overwriteFiles(null, Command.MERGE, IsolationLevel.SERIALIZABLE);
    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Row lineage information is missing");
  }

  @Test
  void createsAppendWriterWithoutLineage() throws Exception {
    SparkWriteBuilder builder = builder(table(), new StructType());
    try (DataWriter<InternalRow> writer =
        builder.build().toBatch().createBatchWriterFactory(() -> 1).createWriter(0, 0)) {
      writer.write(new GenericInternalRow(new Object[] {1}));
      writer.abort();
    }
  }

  @Test
  void preservesCopyOnWriteLineageAfterDateConversion() throws Exception {
    Table table = table();
    SparkWriteBuilder builder = builder(table, lineage());
    builder.overwriteFiles(null, Command.MERGE, IsolationLevel.SERIALIZABLE);
    ProjectingInternalRow metadata =
        new ProjectingInternalRow(
            lineage(), JavaConverters.asScala(List.<Object>of(0, 1)).toIndexedSeq());
    metadata.project(new GenericInternalRow(new Object[] {42L, 3L}));
    SparkWrite.TaskCommit result;
    try (DataWriter<InternalRow> writer =
        builder.build().toBatch().createBatchWriterFactory(() -> 1).createWriter(0, 0)) {
      writer.write(metadata, new GenericInternalRow(new Object[] {1}));
      result = (SparkWrite.TaskCommit) writer.commit();
    }
    assertThat(result.files()).hasSize(1);
    DataFile file = result.files()[0];
    try (CloseableIterable<Record> rows =
        FormatModelRegistry.readBuilder(file.format(), Record.class, table.io().newInputFile(file))
            .project(MetadataColumns.schemaWithRowLineage(table.schema()))
            .idToConstant(
                Map.of(
                    MetadataColumns.ROW_ID.fieldId(),
                    0L,
                    MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(),
                    0L))
            .build()) {
      assertThat(rows)
          .singleElement()
          .satisfies(
              row -> {
                assertThat(row.getField("d")).isEqualTo(LocalDate.ofEpochDay(1).atStartOfDay());
                assertThat(row.getField(MetadataColumns.ROW_ID.name())).isEqualTo(42L);
                assertThat(row.getField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()))
                    .isEqualTo(3L);
              });
    }
  }

  private Table table() {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "d", Types.TimestampType.withoutZone()));
    return new HadoopTables()
        .create(
            schema,
            PartitionSpec.unpartitioned(),
            Map.of("format-version", "3"),
            temp.resolve("t").toString());
  }

  private static StructType lineage() {
    return SparkSchemaUtil.convert(
        new Schema(MetadataColumns.ROW_ID, MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER));
  }

  private static SparkWriteBuilder builder(Table table, StructType metadata) {
    LogicalWriteInfo info = mock(LogicalWriteInfo.class);
    when(info.queryId()).thenReturn("lineage-validation");
    when(info.schema())
        .thenReturn(
            SparkSchemaUtil.convert(
                new Schema(Types.NestedField.optional(1, "d", Types.DateType.get()))));
    when(info.metadataSchema()).thenReturn(Optional.of(metadata));
    when(info.options()).thenReturn(new CaseInsensitiveStringMap(Map.of()));
    return new SparkWriteBuilder(spark, table, null, info);
  }
}
