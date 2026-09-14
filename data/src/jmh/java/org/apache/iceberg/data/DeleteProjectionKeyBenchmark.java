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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/** Measures cache hits without file IO or worker-pool scheduling delays. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
public class DeleteProjectionKeyBenchmark {
  @Param({"1", "200"})
  public int fields;

  @Param({"1", "10"})
  public int files;

  private Schema projection;
  private List<DeleteFile> deletes;
  private BaseDeleteLoader loader;

  @Setup
  public void setup() {
    List<Types.NestedField> columns = Lists.newArrayListWithExpectedSize(fields);
    for (int i = 1; i <= fields; i++) {
      columns.add(
          Types.NestedField.from(
                  Types.NestedField.optional(i, "field_" + i, Types.IntegerType.get()))
              .withInitialDefault(Literal.of(i))
              .build());
    }
    projection = new Schema(columns);
    deletes = Lists.newArrayListWithExpectedSize(files);
    for (int i = 0; i < files; i++) {
      deletes.add(
          FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
              .ofEqualityDeletes(1)
              .withPath("delete-" + i + ".parquet")
              .withFileSizeInBytes(0)
              .withRecordCount(0)
              .build());
    }
    loader = new CacheHitLoader();
    loader.loadEqualityDeletes(deletes, projection);
  }

  @Benchmark
  public StructLikeSet cachedDeletes() {
    return loader.loadEqualityDeletes(deletes, projection);
  }

  private static class CacheHitLoader extends BaseDeleteLoader {
    private final Map<String, Object> cache = new ConcurrentHashMap<>();

    private CacheHitLoader() {
      super(
          file -> {
            throw new IllegalStateException("Unexpected delete file IO");
          },
          MoreExecutors.newDirectExecutorService());
    }

    @Override
    protected boolean canCache(long size) {
      return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <V> V getOrLoad(String key, Supplier<V> supplier, long size) {
      return (V) cache.computeIfAbsent(key, ignored -> List.of());
    }
  }
}
