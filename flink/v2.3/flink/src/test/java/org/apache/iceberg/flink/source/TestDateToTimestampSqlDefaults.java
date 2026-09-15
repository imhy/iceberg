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
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestDateToTimestampSqlDefaults {
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
  void readsPromotedInitialDefaultsAndPreservesExplicitNulls(
      FileFormat format, int precision, boolean modern) {
    TableEnvironment env = createEnvironment(modern);
    sql(
        env,
        "CREATE TABLE t (id INT) WITH ('format-version'='3', 'write.format.default'='%s')",
        format);
    sql(env, "INSERT INTO t VALUES (1)");
    Table table = CATALOG.catalog().loadTable(TableIdentifier.of("db", "t"));
    table
        .updateSchema()
        .addColumn("d", Types.DateType.get(), Literal.of("1969-12-31").to(Types.DateType.get()))
        .commit();
    table
        .updateSchema()
        .updateColumnDefault("d", Literal.of("2021-03-14").to(Types.DateType.get()))
        .commit();
    assertThat(sql(env, "SELECT id, d FROM t"))
        .containsExactly(Row.of(1, LocalDate.parse("1969-12-31")));
    // Flink supplies null for omitted columns; it does not expose Iceberg write defaults to SQL.
    sql(env, "INSERT INTO t (id) VALUES (2)");
    assertThat(sql(env, "SELECT d FROM t WHERE id = 2")).containsExactly(Row.of((Object) null));
    Type.PrimitiveType target =
        precision == 9 ? Types.TimestampNanoType.withoutZone() : Types.TimestampType.withoutZone();
    table.refresh();
    table.updateSchema().updateColumn("d", target).commit();
    assertThat(table.schema().findField("d").initialDefaultLiteral())
        .isEqualTo(Literal.of("1969-12-31T00:00:00").to(target));
    assertThat(table.schema().findField("d").writeDefaultLiteral())
        .isEqualTo(Literal.of("2021-03-14T00:00:00").to(target));
    sql(env, "INSERT INTO t (id) VALUES (3)");
    sql(
        env,
        "INSERT INTO t VALUES (4, CAST(TIMESTAMP '2021-03-14 00:00:00' AS TIMESTAMP(%s)))",
        precision);
    assertThat(sql(env, "SELECT id, d FROM t ORDER BY id"))
        .containsExactly(
            Row.of(1, LocalDate.parse("1969-12-31").atStartOfDay()),
            Row.of(2, null),
            Row.of(3, null),
            Row.of(4, LocalDate.parse("2021-03-14").atStartOfDay()));
    assertThat(
            sql(
                env,
                "SELECT id FROM t WHERE d = TIMESTAMP '1969-12-31 00:00:00' OR d IS NULL ORDER BY id"))
        .containsExactly(Row.of(1), Row.of(2), Row.of(3));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void readsNestedPromotedDefaults(FileFormat format, int precision, boolean modern) {
    TableEnvironment env = createEnvironment(modern);
    sql(
        env,
        "CREATE TABLE t (id INT, s ROW<x INT>, a ARRAY<ROW<x INT>>, m MAP<STRING, ROW<x INT>>) "
            + "WITH ('format-version'='3', 'write.format.default'='%s')",
        format);
    sql(
        env,
        "INSERT INTO t VALUES (1, ROW(1), ARRAY[ROW(1)], MAP['k', ROW(1)]), (2, CAST(NULL AS ROW<x INT>), CAST(NULL AS ARRAY<ROW<x INT>>), CAST(NULL AS MAP<STRING, ROW<x INT>>))");
    Table table = CATALOG.catalog().loadTable(TableIdentifier.of("db", "t"));
    table
        .updateSchema()
        .addColumn(
            "s", "d", Types.DateType.get(), Literal.of("1969-12-31").to(Types.DateType.get()))
        .addColumn(
            "a.element",
            "d",
            Types.DateType.get(),
            Literal.of("1969-12-31").to(Types.DateType.get()))
        .addColumn(
            "m.value", "d", Types.DateType.get(), Literal.of("1969-12-31").to(Types.DateType.get()))
        .commit();
    Type.PrimitiveType target =
        precision == 9 ? Types.TimestampNanoType.withoutZone() : Types.TimestampType.withoutZone();
    table
        .updateSchema()
        .updateColumn("s.d", target)
        .updateColumn("a.element.d", target)
        .updateColumn("m.value.d", target)
        .commit();
    assertThat(sql(env, "SELECT id, s.d, a[1].d, m['k'].d FROM t ORDER BY id"))
        .containsExactly(
            Row.of(
                1,
                LocalDate.parse("1969-12-31").atStartOfDay(),
                LocalDate.parse("1969-12-31").atStartOfDay(),
                LocalDate.parse("1969-12-31").atStartOfDay()),
            Row.of(2, null, null, null));
    assertThat(sql(env, "SELECT id FROM t WHERE s.d = TIMESTAMP '1969-12-31 00:00:00'"))
        .containsExactly(Row.of(1));
  }

  private static TableEnvironment createEnvironment(boolean modern) {
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
    return env;
  }

  private static List<Row> sql(TableEnvironment env, String query, Object... args) {
    return SqlHelpers.sql(env, query, args);
  }
}
