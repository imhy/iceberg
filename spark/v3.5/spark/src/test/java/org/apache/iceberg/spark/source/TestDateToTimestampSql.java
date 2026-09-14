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

import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.stream.Stream;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestDateToTimestampSql {
  @TempDir Path temp;
  private SparkSession spark;

  static Stream<Arguments> modes() {
    return Stream.of("parquet", "avro", "orc")
        .flatMap(
            format ->
                (format.equals("avro") ? Stream.of(false) : Stream.of(false, true))
                    .flatMap(
                        vectorized ->
                            Stream.of("UTC", "America/Los_Angeles")
                                .map(zone -> Arguments.of(format, vectorized, zone))));
  }

  @AfterEach
  void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  private void startSpark(String zone) {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config(TestBase.DISABLE_UI)
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", zone)
            .config("spark.sql.timestampType", "TIMESTAMP_LTZ")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", temp.toUri().toString())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE local.db");
  }

  private void createTable(String columns, String partition, String format, boolean vectorized) {
    spark.sql(
        "CREATE TABLE local.db.t ("
            + columns
            + ") USING iceberg "
            + partition
            + " TBLPROPERTIES ('format-version'='3', 'write.format.default'='"
            + format
            + "', 'write.metadata.metrics.default'='none', 'read.parquet.vectorization.enabled'='"
            + vectorized
            + "', 'read.orc.vectorization.enabled'='"
            + vectorized
            + "')");
  }

  @ParameterizedTest
  @MethodSource("modes")
  void queriesPromotedDatesAndMixedFiles(String format, boolean vectorized, String zone)
      throws Exception {
    startSpark(zone);
    createTable("id INT, d DATE", "PARTITIONED BY (days(d))", format, vectorized);
    spark.sql(
        "INSERT INTO local.db.t VALUES (1, DATE '1969-12-31'), (2, DATE '1970-01-01'), "
            + "(3, DATE '2021-03-14'), (4, CAST(NULL AS DATE))");
    Table table = Spark3Util.loadIcebergTable(spark, "local.db.t");
    int specId = table.spec().specId();
    spark.sql("ALTER TABLE local.db.t ALTER COLUMN d TYPE TIMESTAMP_NTZ");
    spark.sql("REFRESH TABLE local.db.t");
    table.refresh();
    assertThat(table.schema().findType("d")).isEqualTo(Types.TimestampType.withoutZone());
    assertThat(table.spec().specId()).isEqualTo(specId);
    assertThat(spark.sql("SELECT id, d FROM local.db.t ORDER BY id").collectAsList())
        .containsExactly(
            RowFactory.create(1, midnight("1969-12-31")),
            RowFactory.create(2, midnight("1970-01-01")),
            RowFactory.create(3, midnight("2021-03-14")),
            RowFactory.create(4, null));
    spark.sql("INSERT INTO local.db.t VALUES (5, TIMESTAMP_NTZ '2021-03-14 01:30:00.123456')");
    spark.sql("INSERT INTO local.db.t VALUES (6, DATE '1970-01-02')");
    assertThat(spark.sql("SELECT id, d FROM local.db.t ORDER BY id").collectAsList())
        .containsExactly(
            RowFactory.create(1, midnight("1969-12-31")),
            RowFactory.create(2, midnight("1970-01-01")),
            RowFactory.create(3, midnight("2021-03-14")),
            RowFactory.create(4, null),
            RowFactory.create(5, LocalDateTime.parse("2021-03-14T01:30:00.123456")),
            RowFactory.create(6, midnight("1970-01-02")));
    assertThat(
            spark
                .sql(
                    "SELECT id FROM local.db.t WHERE (d >= TIMESTAMP_NTZ '1970-01-01 00:00:00' "
                        + "AND d < TIMESTAMP_NTZ '2021-03-14 00:00:00') OR d IS NULL "
                        + "OR d = TIMESTAMP_NTZ '2021-03-14 01:30:00.123456' ORDER BY id")
                .collectAsList())
        .containsExactly(
            RowFactory.create(2), RowFactory.create(4), RowFactory.create(5), RowFactory.create(6));
    assertThat(
            spark
                .sql(
                    "SELECT id FROM local.db.t WHERE NOT (d IN (TIMESTAMP_NTZ '1970-01-01 00:00:00', "
                        + "TIMESTAMP_NTZ '2021-03-14 00:00:00')) ORDER BY id")
                .collectAsList())
        .containsExactly(RowFactory.create(1), RowFactory.create(5), RowFactory.create(6));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void preservesInitialDefaultsAndRejectsUnsupportedDefaultWrites(
      String format, boolean vectorized, String zone) throws Exception {
    startSpark(zone);
    createTable("id INT", "", format, vectorized);
    spark.sql("INSERT INTO local.db.t VALUES (1)");
    Table table = Spark3Util.loadIcebergTable(spark, "local.db.t");
    Literal<?> initial = Literal.of("1969-12-31").to(Types.DateType.get());
    Literal<?> write = Literal.of("2021-03-14").to(Types.DateType.get());
    table
        .updateSchema()
        .addColumn("d", Types.DateType.get(), initial)
        .addRequiredColumn("r", Types.DateType.get(), null, initial)
        .commit();
    table.updateSchema().updateColumnDefault("d", write).updateColumnDefault("r", write).commit();
    spark.sql("REFRESH TABLE local.db.t");
    // This Spark integration does not expose write defaults, even before promotion.
    assertThatThrownBy(() -> spark.sql("INSERT INTO local.db.t (id) VALUES (2)"))
        .hasMessageContaining("Cannot find data for the output column");
    spark.sql("ALTER TABLE local.db.t ALTER COLUMN d TYPE TIMESTAMP_NTZ");
    spark.sql("ALTER TABLE local.db.t ALTER COLUMN r TYPE TIMESTAMP_NTZ");
    spark.sql("REFRESH TABLE local.db.t");
    assertThatThrownBy(() -> spark.sql("INSERT INTO local.db.t (id) VALUES (2)"))
        .hasMessageContaining("Cannot find data for the output column");
    spark.sql(
        "INSERT INTO local.db.t VALUES (2, TIMESTAMP_NTZ '2021-03-14 00:00:00', "
            + "TIMESTAMP_NTZ '2021-03-14 00:00:00'), (3, NULL, TIMESTAMP_NTZ '2021-03-14 00:00:00')");
    assertThat(spark.sql("SELECT id, d, r FROM local.db.t ORDER BY id").collectAsList())
        .containsExactly(
            RowFactory.create(1, midnight("1969-12-31"), midnight("1969-12-31")),
            RowFactory.create(2, midnight("2021-03-14"), midnight("2021-03-14")),
            RowFactory.create(3, null, midnight("2021-03-14")));
    assertThat(
            spark
                .sql(
                    "SELECT id FROM local.db.t WHERE d = TIMESTAMP_NTZ '1969-12-31 00:00:00' "
                        + "OR d IS NULL ORDER BY id")
                .collectAsList())
        .containsExactly(RowFactory.create(1), RowFactory.create(3));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void queriesNestedPromotedDates(String format, boolean vectorized, String zone) throws Exception {
    startSpark(zone);
    createTable(
        "id INT, s STRUCT<d:DATE>, a ARRAY<DATE>, m MAP<STRING,DATE>", "", format, vectorized);
    spark.sql(
        "INSERT INTO local.db.t VALUES "
            + "(1, named_struct('d', DATE '1969-12-31'), array(DATE '1970-01-01', NULL), "
            + "map('k', DATE '2021-03-14')), (2, NULL, NULL, NULL)");
    Table table = Spark3Util.loadIcebergTable(spark, "local.db.t");
    table
        .updateSchema()
        .updateColumn("s.d", Types.TimestampType.withoutZone())
        .updateColumn("a.element", Types.TimestampType.withoutZone())
        .updateColumn("m.value", Types.TimestampType.withoutZone())
        .commit();
    spark.sql("REFRESH TABLE local.db.t");
    assertThat(
            spark
                .sql("SELECT id, s.d, a[0], a[1], m['k'] FROM local.db.t ORDER BY id")
                .collectAsList())
        .containsExactly(
            RowFactory.create(
                1, midnight("1969-12-31"), midnight("1970-01-01"), null, midnight("2021-03-14")),
            RowFactory.create(2, null, null, null, null));
    assertThat(
            spark
                .sql("SELECT id FROM local.db.t WHERE s.d < TIMESTAMP_NTZ '1970-01-01 00:00:00'")
                .collectAsList())
        .containsExactly(RowFactory.create(1));
  }

  private static LocalDateTime midnight(String date) {
    return LocalDate.parse(date).atStartOfDay();
  }
}
