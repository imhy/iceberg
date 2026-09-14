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
package org.apache.iceberg.flink.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestDateToTimestampSql {
  @RegisterExtension
  static final MiniClusterExtension MINI_CLUSTER =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @RegisterExtension
  static final HadoopCatalogExtension CATALOG = new HadoopCatalogExtension("db", "t");

  static Stream<Arguments> modes() {
    return Stream.of(FileFormat.PARQUET, FileFormat.AVRO, FileFormat.ORC)
        .flatMap(
            format ->
                Stream.of(6, 9)
                    .flatMap(
                        precision ->
                            Stream.of(false, true)
                                .map(modern -> Arguments.of(format, precision, modern))));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void queriesPromotedDatesAndNewTimestamps(FileFormat format, int precision, boolean modern) {
    TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
    env.getConfig().setLocalTimeZone(ZoneId.of(modern ? "America/Los_Angeles" : "UTC"));
    env.getConfig()
        .getConfiguration()
        .set(CoreOptions.DEFAULT_PARALLELISM, 1)
        .set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_FLIP27_SOURCE, modern)
        .set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_V2_SINK, modern);
    sql(
        env,
        "CREATE CATALOG promotion WITH ('type'='iceberg', 'catalog-type'='hadoop', "
            + "'warehouse'='%s', 'cache-enabled'='false')",
        CATALOG.warehouse());
    sql(env, "USE CATALOG promotion");
    sql(env, "CREATE DATABASE db");
    sql(env, "USE db");
    sql(
        env,
        "CREATE TABLE t (id INT, d DATE) WITH ('format-version'='3', "
            + "'write.format.default'='%s', 'write.metadata.metrics.default'='none')",
        format);
    sql(
        env,
        "INSERT INTO t VALUES (1, DATE '1969-12-31'), (2, DATE '1970-01-01'), "
            + "(3, DATE '2021-03-14'), (4, CAST(NULL AS DATE))");

    Table table = CATALOG.catalog().loadTable(TableIdentifier.of("db", "t"));
    Type.PrimitiveType target =
        precision == 9 ? Types.TimestampNanoType.withoutZone() : Types.TimestampType.withoutZone();
    table.updateSchema().updateColumn("d", target).commit();
    table.refresh();
    assertThat(table.schema().findType("d")).isEqualTo(target);
    assertThat(sql(env, "SELECT id, d FROM t ORDER BY id"))
        .containsExactly(
            Row.of(1, midnight("1969-12-31")),
            Row.of(2, midnight("1970-01-01")),
            Row.of(3, midnight("2021-03-14")),
            Row.of(4, null));

    String timestamp =
        precision == 9 ? "2021-03-14 01:30:00.123456789" : "2021-03-14 01:30:00.123456";
    sql(
        env,
        "INSERT INTO t VALUES (5, CAST(TIMESTAMP '%s' AS TIMESTAMP(%s)))",
        timestamp,
        precision);
    sql(env, "INSERT INTO t VALUES (6, CAST(DATE '1970-01-02' AS TIMESTAMP(%s)))", precision);
    assertThat(sql(env, "SELECT id, d FROM t ORDER BY id"))
        .containsExactly(
            Row.of(1, midnight("1969-12-31")),
            Row.of(2, midnight("1970-01-01")),
            Row.of(3, midnight("2021-03-14")),
            Row.of(4, null),
            Row.of(5, LocalDateTime.parse(timestamp.replace(' ', 'T'))),
            Row.of(6, midnight("1970-01-02")));
    assertThat(
            sql(
                env,
                "SELECT id FROM t WHERE (d >= TIMESTAMP '1970-01-01 00:00:00' "
                    + "AND d < TIMESTAMP '2021-03-14 00:00:00') OR d IS NULL "
                    + "OR d = CAST(TIMESTAMP '%s' AS TIMESTAMP(%s)) ORDER BY id",
                timestamp,
                precision))
        .containsExactly(Row.of(2), Row.of(4), Row.of(5), Row.of(6));
  }

  private static LocalDateTime midnight(String date) {
    return LocalDate.parse(date).atStartOfDay();
  }

  private static List<Row> sql(TableEnvironment env, String query, Object... args) {
    return SqlHelpers.sql(env, query, args);
  }
}
