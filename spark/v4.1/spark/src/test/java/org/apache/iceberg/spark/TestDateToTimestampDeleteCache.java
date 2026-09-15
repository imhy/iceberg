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
package org.apache.iceberg.spark;

import static org.apache.iceberg.Files.localOutput;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.spark.TestSparkExecutorCache.CustomFileIO;
import org.apache.iceberg.spark.TestSparkExecutorCache.CustomInputFile;
import org.apache.iceberg.spark.source.SerializableTableWithSize;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestDateToTimestampDeleteCache {
  @TempDir Path temp;
  private SparkSession spark;
  private String tableName;

  static Stream<Arguments> modes() {
    return Stream.of("parquet", "avro", "orc")
        .flatMap(format -> Stream.of(false, true).map(cache -> Arguments.of(format, cache)));
  }

  @AfterEach
  void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @ParameterizedTest
  @MethodSource("modes")
  void appliesDeletesAcrossLiveProjectionsAndCacheCleanup(String format, boolean cache)
      throws Exception {
    Table table = createTable(format, cache);
    DeleteFile deleteFile = addDeletes(table, format);

    SparkEnv.get().blockManager().memoryStore().clear();

    // Retain the RDDs so their table broadcasts stay live across the schema change.
    JavaRDD<Row> oldWide = spark.table(tableName).select("id", "a", "d").javaRDD();
    JavaRDD<Row> oldNarrow = spark.table(tableName).select("id").javaRDD();
    List<Row> dates = oldWide.collect();
    assertThat(dates).extracting(row -> row.getInt(0)).containsExactlyInAnyOrder(2, 3);
    List<Row> ids = List.of(RowFactory.create(2), RowFactory.create(3));
    checkProjection(oldNarrow, ids);
    int beforeRepeat = streamCount(deleteFile);
    checkProjection(oldWide, dates);
    checkProjection(oldNarrow, ids);
    checkReads(deleteFile, beforeRepeat, cache);

    table.updateSchema().updateColumn("d", Types.TimestampType.withoutZone()).commit();
    sql("REFRESH TABLE %s");
    int beforePromotionRead = streamCount(deleteFile);
    JavaRDD<Row> newNarrow = spark.table(tableName).select("id").javaRDD();
    JavaRDD<Row> newWide = spark.table(tableName).select("id", "a", "d").javaRDD();
    checkProjection(newNarrow, ids);
    List<Row> timestamps =
        List.of(
            RowFactory.create(2, 8, LocalDate.ofEpochDay(1).atStartOfDay()),
            RowFactory.create(3, 7, LocalDate.ofEpochDay(-1).atStartOfDay()));
    checkProjection(newWide, timestamps);
    assertThat(streamCount(deleteFile)).isGreaterThan(beforePromotionRead);

    int afterPromotionRead = streamCount(deleteFile);
    checkProjection(oldWide, dates);
    checkProjection(oldNarrow, ids);
    checkProjection(newWide, timestamps);
    checkProjection(newNarrow, ids);
    checkReads(deleteFile, afterPromotionRead, cache);

    // Exercise the broadcast cleanup callback, then read through both still-live projections.
    try (AutoCloseable ignored = (AutoCloseable) SerializableTableWithSize.copyOf(table)) {
      checkProjection(newNarrow, ids);
    }
    int afterCleanup = streamCount(deleteFile);
    checkProjection(newWide, timestamps);
    checkProjection(oldWide, dates);
    assertThat(streamCount(deleteFile)).isGreaterThan(afterCleanup);
  }

  private Table createTable(String format, boolean cache) throws Exception {
    tableName = "local.db.t_" + format + "_" + cache;
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config(TestBase.DISABLE_UI)
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "2")
            // Keep broadcast cleanup deterministic; the test invokes its callback explicitly.
            .config("spark.cleaner.referenceTracking", "false")
            .config(SparkSQLProperties.EXECUTOR_CACHE_ENABLED, "true")
            .config(SparkSQLProperties.EXECUTOR_CACHE_DELETE_FILES_ENABLED, Boolean.toString(cache))
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.io-impl", CustomFileIO.class.getName())
            .config("spark.sql.catalog.local.warehouse", temp.toUri().toString())
            .getOrCreate();
    sql("CREATE NAMESPACE local.db");
    sql(
        "CREATE TABLE %s (id INT, a INT, d DATE) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3', 'write.format.default'='"
            + format
            + "', 'write.delete.format.default'='"
            + format
            + "')");
    // Concurrent local writers can race while creating their common parent directory.
    Files.createDirectories(temp.resolve("db/t_" + format + "_" + cache + "/data"));
    sql(
        "INSERT INTO %s VALUES (1, 7, DATE '1970-01-02'), (2, 8, DATE '1970-01-02'), "
            + "(3, 7, DATE '1969-12-31'), (4, 7, CAST(NULL AS DATE))");
    return Spark3Util.loadIcebergTable(spark, tableName);
  }

  private DeleteFile addDeletes(Table table, String format) throws Exception {
    Schema deleteSchema = new Schema(table.schema().findField("d"), table.schema().findField("a"));
    Record delete = GenericRecord.create(deleteSchema);
    DeleteFile deleteFile =
        FileHelpers.writeDeleteFile(
            table,
            localOutput(temp.resolve("deletes." + format).toString()),
            List.of(
                delete.copy("d", LocalDate.ofEpochDay(1), "a", 7), delete.copy("d", null, "a", 7)),
            deleteSchema);
    table.newRowDelta().addDeletes(deleteFile).commit();
    sql("REFRESH TABLE %s");
    return deleteFile;
  }

  private static void checkProjection(JavaRDD<Row> projection, List<Row> expected) {
    assertThat(projection.collect()).containsExactlyInAnyOrderElementsOf(expected);
  }

  private void sql(String query) {
    spark.sql(String.format(Locale.ROOT, query, tableName));
  }

  private static void checkReads(DeleteFile file, int previous, boolean cache) {
    if (cache) {
      assertThat(streamCount(file))
          .as("Cached delete rows should avoid reopening the file")
          .isEqualTo(previous);
    } else {
      assertThat(streamCount(file))
          .as("Disabling delete caching should reopen the file")
          .isGreaterThan(previous);
    }
  }

  private static int streamCount(DeleteFile file) {
    return ((CustomInputFile) new CustomFileIO().newInputFile(file.location())).streamCount();
  }
}
