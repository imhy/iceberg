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
package org.apache.iceberg.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestDateToTimestampSink {
  @RegisterExtension
  static final MiniClusterExtension MINI_CLUSTER =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @RegisterExtension
  static final HadoopCatalogExtension CATALOG = new HadoopCatalogExtension("db", "t");

  static Stream<Arguments> modes() {
    return Stream.of(Types.TimestampType.withoutZone(), Types.TimestampNanoType.withoutZone())
        .flatMap(
            target ->
                Stream.of(FileFormat.PARQUET, FileFormat.AVRO, FileFormat.ORC)
                    .flatMap(
                        format ->
                            Stream.of(false, true)
                                .map(modern -> Arguments.of(target, format, modern))));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void writesRawDatesThroughTheSink(Type.PrimitiveType target, FileFormat format, boolean modern)
      throws Exception {
    Schema schema = schema(target);
    Table table =
        CATALOG
            .catalog()
            .createTable(
                TableIdentifier.of("db", "t"),
                schema,
                PartitionSpec.builderFor(schema).hour("d").build(),
                Map.of("format-version", "3", "write.format.default", format.name()));
    StreamExecutionEnvironment env = environment();
    attach(env, table, modern);
    env.execute("write promoted dates");
    table.refresh();
    List<Object> values = new ArrayList<>();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      for (Record row : rows) {
        values.add(row.getField("d"));
      }
    }
    assertThat(values)
        .containsExactlyInAnyOrder(
            LocalDate.ofEpochDay(-1).atStartOfDay(), LocalDate.ofEpochDay(1).atStartOfDay(), null);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectsDateInputUsingTheActualV2TableVersion(boolean modern) {
    Schema schema = schema(Types.TimestampType.withoutZone());
    Table table =
        CATALOG
            .catalog()
            .createTable(
                TableIdentifier.of("db", "t"),
                schema,
                PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"));
    assertThatThrownBy(() -> attach(environment(), table, modern))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("date cannot be promoted to timestamp");
  }

  private static StreamExecutionEnvironment environment() {
    return StreamExecutionEnvironment.getExecutionEnvironment(
            MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
        .setRuntimeMode(RuntimeExecutionMode.BATCH)
        .setParallelism(2)
        .setMaxParallelism(2);
  }

  private static void attach(StreamExecutionEnvironment env, Table table, boolean modern) {
    RowType dates = FlinkSchemaUtil.convert(schema(Types.DateType.get()));
    List<RowData> values =
        List.of(GenericRowData.of(1, -1), GenericRowData.of(2, 1), GenericRowData.of(3, null));
    DataStream<RowData> input = env.fromData(values, FlinkCompatibilityUtil.toTypeInfo(dates));
    if (modern) {
      IcebergSink.forRowData(input)
          .table(table)
          .tableLoader(CATALOG.tableLoader())
          .resolvedSchema(FlinkSchemaUtil.toResolvedSchema(dates))
          .distributionMode(DistributionMode.HASH)
          .append();
    } else {
      FlinkSink.forRowData(input)
          .table(table)
          .tableLoader(CATALOG.tableLoader())
          .resolvedSchema(FlinkSchemaUtil.toResolvedSchema(dates))
          .distributionMode(DistributionMode.HASH)
          .append();
    }
  }

  private static Schema schema(Type type) {
    return new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "d", type));
  }
}
