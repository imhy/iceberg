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

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestDeleteLoaderCache {
  @TempDir private File temp;

  static Stream<Arguments> promotions() {
    return Stream.of(false, true)
        .flatMap(
            cache ->
                Stream.of(
                    Arguments.of(
                        cache,
                        Types.DateType.get(),
                        Types.TimestampType.withoutZone(),
                        LocalDate.ofEpochDay(1),
                        LocalDate.ofEpochDay(1).atStartOfDay()),
                    Arguments.of(
                        cache,
                        Types.DateType.get(),
                        Types.TimestampNanoType.withoutZone(),
                        LocalDate.ofEpochDay(1),
                        LocalDate.ofEpochDay(1).atStartOfDay()),
                    Arguments.of(cache, Types.IntegerType.get(), Types.LongType.get(), 34, 34L)));
  }

  @AfterEach
  void clearTables() {
    TestTables.clearTables();
  }

  @ParameterizedTest
  @MethodSource("promotions")
  void equalityDeletesAfterPromotion(
      boolean cache,
      Type.PrimitiveType from,
      Type.PrimitiveType to,
      Object oldValue,
      Object newValue)
      throws IOException {
    Schema original = new Schema(Types.NestedField.optional(1, "value", from));
    Table table = create(original);
    DeleteFile delete = write(table, original, row(original, oldValue));
    CachingLoader loader = new CachingLoader(table, cache);
    assertContains(loader, delete, original, oldValue);
    table.updateSchema().updateColumn("value", to).commit();
    Schema promoted = table.schema();
    assertContains(loader, delete, promoted, newValue);
    assertContains(loader, delete, original, oldValue);
    assertContains(loader, delete, promoted, newValue);
    assertThat(loader.loads()).isEqualTo(cache ? 2 : 4);
  }

  @Test
  void equalityDeletesWithReorderedFields() throws IOException {
    Schema original =
        new Schema(
            Types.NestedField.optional(1, "a", Types.IntegerType.get()),
            Types.NestedField.optional(2, "b", Types.IntegerType.get()));
    Table table = create(original);
    DeleteFile delete = write(table, original, row(original, 1, 2));
    CachingLoader loader = new CachingLoader(table, true);
    Schema reordered = new Schema(original.findField("b"), original.findField("a"));
    assertContains(loader, delete, original, 1, 2);
    assertContains(loader, delete, reordered, 2, 1);
    assertContains(loader, delete, original, 1, 2);
    assertThat(loader.loads()).isEqualTo(2);
  }

  @Test
  void equalityDeletesWithDifferentInitialDefaults() throws IOException {
    Schema original = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    Table table = create(original);
    DeleteFile delete = write(table, original, row(original, 1));
    CachingLoader loader = new CachingLoader(table, true);
    for (int value : new int[] {2, 3, 2}) {
      Schema projection =
          new Schema(
              original.findField("id"),
              Types.NestedField.optional("missing")
                  .withId(2)
                  .ofType(Types.IntegerType.get())
                  .withInitialDefault(Literal.of(value))
                  .build());
      assertContains(loader, delete, projection, 1, value);
    }
    assertThat(loader.loads()).isEqualTo(2);
  }

  @Test
  void isolatesConcurrentProjections() throws Exception {
    Schema original = new Schema(Types.NestedField.optional(1, "value", Types.DateType.get()));
    Table table = create(original);
    DeleteFile delete = write(table, original, row(original, LocalDate.ofEpochDay(1)));
    table.updateSchema().updateColumn("value", Types.TimestampType.withoutZone()).commit();
    Schema promoted = table.schema();
    CachingLoader loader = new CachingLoader(table, true);
    ExecutorService workers = Executors.newFixedThreadPool(4);
    try {
      List<Future<?>> results = Lists.newArrayList();
      for (int i = 0; i < 20; i++) {
        results.add(
            workers.submit(
                () -> assertContains(loader, delete, original, LocalDate.ofEpochDay(1))));
        results.add(
            workers.submit(
                () ->
                    assertContains(
                        loader, delete, promoted, LocalDate.ofEpochDay(1).atStartOfDay())));
      }
      for (Future<?> result : results) {
        result.get();
      }
      assertThat(loader.loads()).isEqualTo(2);
    } finally {
      workers.shutdownNow();
    }
  }

  @Test
  void positionDeletesStillReuseCachedFile() throws IOException {
    Table table = create(new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get())));
    DeleteFile delete =
        FileHelpers.writeDeleteFile(
                table,
                Files.localOutput(new File(temp, "positions.parquet")),
                List.of(Pair.of("data.parquet", 2L)))
            .first();
    CachingLoader loader = new CachingLoader(table, true);
    for (int attempt = 0; attempt < 2; attempt += 1) {
      PositionDeleteIndex index = loader.loadPositionDeletes(List.of(delete), "data.parquet");
      assertThat(index.isDeleted(2)).isTrue();
      assertThat(index.isDeleted(1)).isFalse();
    }
    assertThat(loader.loads()).isEqualTo(1);
  }

  private Table create(Schema schema) {
    return TestTables.create(
        new File(temp, "table"), "test", schema, PartitionSpec.unpartitioned(), 3);
  }

  private DeleteFile write(Table table, Schema schema, Record record) throws IOException {
    return FileHelpers.writeDeleteFile(
        table, Files.localOutput(new File(temp, "deletes.parquet")), List.of(record), schema);
  }

  private static Record row(Schema schema, Object... values) {
    GenericRecord record = GenericRecord.create(schema);
    for (int pos = 0; pos < values.length; pos += 1) {
      record.set(pos, values[pos]);
    }
    return record;
  }

  private static void assertContains(
      BaseDeleteLoader loader, DeleteFile delete, Schema schema, Object... values) {
    StructLikeSet deletes = loader.loadEqualityDeletes(List.of(delete), schema);
    assertThat(deletes).hasSize(1);
    assertThat(
            deletes.contains(
                new InternalRecordWrapper(schema.asStruct()).wrap(row(schema, values))))
        .isTrue();
  }

  private static class CachingLoader extends BaseDeleteLoader {
    private final Map<String, Object> cache = Maps.newConcurrentMap();
    private final AtomicInteger loads;
    private final boolean enabled;

    private CachingLoader(Table table, boolean enabled) {
      this(table, enabled, new AtomicInteger());
    }

    private CachingLoader(Table table, boolean enabled, AtomicInteger loads) {
      super(
          file -> {
            loads.incrementAndGet();
            return table.io().newInputFile(file.location());
          });
      this.enabled = enabled;
      this.loads = loads;
    }

    private int loads() {
      return loads.get();
    }

    @Override
    protected boolean canCache(long size) {
      return enabled;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <V> V getOrLoad(String key, Supplier<V> supplier, long size) {
      return (V) cache.computeIfAbsent(key, ignored -> supplier.get());
    }
  }
}
