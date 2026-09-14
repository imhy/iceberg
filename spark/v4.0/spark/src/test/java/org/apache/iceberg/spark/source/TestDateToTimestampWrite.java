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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestDateToTimestampWrite {
  @TempDir Path temp;
  private SparkSession spark;

  static Stream<Arguments> writes() {
    return Stream.of("parquet", "avro", "orc")
        .flatMap(
            format ->
                Stream.of(
                        "",
                        "PARTITIONED BY (hours(d))",
                        "PARTITIONED BY (bucket(4, d))",
                        "PARTITIONED BY (d)")
                    .flatMap(
                        partition ->
                            Stream.of("hash", "range")
                                .flatMap(
                                    distribution ->
                                        Stream.of(false, true)
                                            .map(
                                                merge ->
                                                    Arguments.of(
                                                        format, partition, distribution, merge)))));
  }

  private void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("date-promotion-writes")
            .config(TestBase.DISABLE_UI)
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "America/Los_Angeles")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", temp.toUri().toString())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE local.db");
  }

  @AfterEach
  void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @ParameterizedTest
  @MethodSource("writes")
  void writesDateInput(String format, String partition, String distribution, boolean merge)
      throws Exception {
    startSpark();
    spark.sql(
        "CREATE TABLE local.db.t (id BIGINT, d TIMESTAMP_NTZ) USING iceberg "
            + partition
            + " TBLPROPERTIES ('format-version'='3', 'write.spark.accept-any-schema'='true', "
            + "'write.format.default'='"
            + format
            + "', 'write.distribution-mode'='"
            + distribution
            + "')");
    Table table = Spark3Util.loadIcebergTable(spark, "local.db.t");
    table.replaceSortOrder().asc("d").commit();
    spark.sql("REFRESH TABLE local.db.t");
    spark
        .sql(
            "SELECT id, CASE WHEN id = 100 THEN CAST(NULL AS DATE) ELSE date_add(DATE '1970-01-01', CAST(id % 13 - 6 AS INT)) END AS d FROM range(101)")
        .repartition(2)
        .writeTo("local.db.t")
        .option("merge-schema", Boolean.toString(merge))
        .append();
    table.refresh();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      PartitionKey key = new PartitionKey(table.spec(), table.schema());
      InternalRecordWrapper wrapper = new InternalRecordWrapper(table.schema().asStruct());
      for (FileScanTask task : tasks) {
        assertThat(task.file().sortOrderId()).isEqualTo(table.sortOrder().orderId());
        List<LocalDateTime> timestamps = new ArrayList<>();
        try (CloseableIterable<Record> rows =
            FormatModelRegistry.readBuilder(
                    task.file().format(), Record.class, table.io().newInputFile(task.file()))
                .project(table.schema())
                .build()) {
          for (Record row : rows) {
            LocalDateTime timestamp = (LocalDateTime) row.getField("d");
            long id = (Long) row.getField("id");
            assertThat(timestamp)
                .isEqualTo(id == 100 ? null : LocalDate.ofEpochDay(id % 13 - 6).atStartOfDay());
            timestamps.add(timestamp);
            key.partition(wrapper.wrap(row));
            assertThat(key.toPath())
                .isEqualTo(table.spec().partitionToPath(task.file().partition()));
          }
        }
        assertThat(timestamps)
            .isSortedAccordingTo(Comparator.nullsFirst(Comparator.naturalOrder()));
      }
    }
    assertThat(table.schema().findType("d")).isEqualTo(Types.TimestampType.withoutZone());
    assertThat(spark.sql("SELECT count(*) FROM local.db.t").head().getLong(0)).isEqualTo(101);
    assertThat(spark.sql("SELECT d FROM local.db.t WHERE id = 0").head().get(0))
        .isEqualTo(LocalDateTime.parse("1969-12-26T00:00:00"));
    assertThat(
            spark
                .sql(
                    "SELECT count(*) FROM local.db.t WHERE d = TIMESTAMP_NTZ '1969-12-26 00:00:00'")
                .head()
                .getLong(0))
        .isEqualTo(8);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectsRawDateInputForV2(boolean merge) {
    startSpark();
    spark.sql(
        "CREATE TABLE local.db.t (d TIMESTAMP_NTZ) USING iceberg TBLPROPERTIES "
            + "('format-version'='2', 'write.spark.accept-any-schema'='true')");
    assertThatThrownBy(
            () ->
                spark
                    .sql("SELECT DATE '1970-01-02' AS d")
                    .writeTo("local.db.t")
                    .option("merge-schema", Boolean.toString(merge))
                    .append())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            merge
                ? "Cannot change column type: d: timestamp -> date"
                : "date cannot be promoted to timestamp");
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "avro", "orc"})
  void convertsNestedDatesAndPreservesNulls(String format) throws Exception {
    startSpark();
    spark.sql(
        "CREATE TABLE local.db.t (id BIGINT, s STRUCT<d: TIMESTAMP_NTZ>, "
            + "l ARRAY<TIMESTAMP_NTZ>, m MAP<INT, TIMESTAMP_NTZ>) USING iceberg PARTITIONED BY (hours(s.d)) "
            + "TBLPROPERTIES ('format-version'='3', 'write.spark.accept-any-schema'='true', "
            + "'write.format.default'='"
            + format
            + "')");
    spark
        .sql(
            "SELECT CAST(1 AS BIGINT) AS id, named_struct('d', DATE '1970-01-02') AS s, "
                + "array(DATE '1969-12-31', CAST(NULL AS DATE)) AS l, map(1, DATE '1970-01-01', 2, CAST(NULL AS DATE)) AS m "
                + "UNION ALL SELECT CAST(2 AS BIGINT), NULL, NULL, NULL")
        .writeTo("local.db.t")
        .option("merge-schema", "true")
        .append();
    Table table = Spark3Util.loadIcebergTable(spark, "local.db.t");
    List<Record> actual = new ArrayList<>();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      rows.forEach(actual::add);
    }
    assertThat(actual).hasSize(2);
    Record present =
        actual.stream().filter(row -> row.getField("id").equals(1L)).findFirst().orElseThrow();
    assertThat(((Record) present.getField("s")).getField("d"))
        .isEqualTo(LocalDate.ofEpochDay(1).atStartOfDay());
    assertThat(present.getField("l"))
        .asList()
        .containsExactly(LocalDate.ofEpochDay(-1).atStartOfDay(), null);
    Map<?, ?> map = (Map<?, ?>) present.getField("m");
    assertThat(map.get(1)).isEqualTo(LocalDate.ofEpochDay(0).atStartOfDay());
    assertThat(map.get(2)).isNull();
    Record absent =
        actual.stream().filter(row -> row.getField("id").equals(2L)).findFirst().orElseThrow();
    assertThat(absent.getField("s")).isNull();
    assertThat(absent.getField("l")).isNull();
    assertThat(absent.getField("m")).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "PARTITIONED BY (hours(d))", "PARTITIONED BY (bucket(4, d))"})
  void rejectsOverflowBeforeCommitting(String partition) throws Exception {
    startSpark();
    spark.sql(
        "CREATE TABLE local.db.t (d TIMESTAMP_NTZ) USING iceberg "
            + partition
            + " TBLPROPERTIES ('format-version'='3', 'write.spark.accept-any-schema'='true')");
    Table table = Spark3Util.loadIcebergTable(spark, "local.db.t");
    for (long days :
        new long[] {Long.MIN_VALUE / 86_400_000_000L - 1, Long.MAX_VALUE / 86_400_000_000L + 1}) {
      assertThatThrownBy(
              () ->
                  spark
                      .sql("SELECT date_add(DATE '1970-01-01', " + days + ") AS d")
                      .writeTo("local.db.t")
                      .option("merge-schema", "true")
                      .append())
          .hasRootCauseInstanceOf(ArithmeticException.class);
      table.refresh();
      assertThat(table.currentSnapshot()).isNull();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "avro", "orc"})
  void writesDatesAtTheTimestampRangeBoundaries(String format) throws Exception {
    startSpark();
    spark.sql(
        "CREATE TABLE local.db.t (d TIMESTAMP_NTZ) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3', 'write.spark.accept-any-schema'='true', "
            + "'write.format.default'='"
            + format
            + "')");
    long minDays = Long.MIN_VALUE / 86_400_000_000L;
    long maxDays = Long.MAX_VALUE / 86_400_000_000L;
    spark
        .sql(
            "SELECT date_add(DATE '1970-01-01', "
                + minDays
                + ") AS d UNION ALL "
                + "SELECT date_add(DATE '1970-01-01', "
                + maxDays
                + ")")
        .writeTo("local.db.t")
        .option("merge-schema", "true")
        .append();
    assertThat(spark.sql("SELECT d FROM local.db.t").collectAsList())
        .extracting(row -> row.get(0))
        .containsExactlyInAnyOrder(
            LocalDate.ofEpochDay(minDays).atStartOfDay(),
            LocalDate.ofEpochDay(maxDays).atStartOfDay());
  }
}
