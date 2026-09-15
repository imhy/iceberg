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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TestSparkExecutorCacheKeySize {
  @Test
  void includesKeyWhenAdmittingEntries() {
    SparkExecutorCache cache = cache(256, 4096);
    AtomicInteger loads = new AtomicInteger();
    String wideKey = "k".repeat(200);
    cache.getOrLoad("group", wideKey, loads::incrementAndGet, 10);
    cache.getOrLoad("group", wideKey, loads::incrementAndGet, 10);
    assertThat(loads).hasValue(2);
    cache.getOrLoad("group", "small", loads::incrementAndGet, 10);
    cache.getOrLoad("group", "small", loads::incrementAndGet, 10);
    assertThat(loads).hasValue(3);
  }

  @Test
  void includesKeysInEvictionBudget() throws Exception {
    SparkExecutorCache cache = cache(1000, 400);
    cache.getOrLoad("group", "a".repeat(100), Object::new, 10);
    cache.getOrLoad("group", "b".repeat(100), Object::new, 10);
    Field field = SparkExecutorCache.class.getDeclaredField("state");
    field.setAccessible(true);
    Cache<?, ?> state = (Cache<?, ?>) field.get(cache);
    state.cleanUp();
    // Both values fit, but their retained keys require eviction of one entry.
    assertThat(state.asMap()).hasSize(1);
  }

  @Test
  void avoidsOverflowWhenAddingKeySize() {
    SparkExecutorCache cache = cache(Long.MAX_VALUE, Long.MAX_VALUE);
    AtomicInteger loads = new AtomicInteger();
    cache.getOrLoad("group", "key", loads::incrementAndGet, Long.MAX_VALUE - 1);
    cache.getOrLoad("group", "key", loads::incrementAndGet, Long.MAX_VALUE - 1);
    assertThat(loads).hasValue(2);
  }

  private static SparkExecutorCache cache(long maxEntrySize, long maxTotalSize) {
    SparkExecutorCache.Conf conf = mock(SparkExecutorCache.Conf.class);
    when(conf.timeout()).thenReturn(Duration.ofHours(1));
    when(conf.maxEntrySize()).thenReturn(maxEntrySize);
    when(conf.maxTotalSize()).thenReturn(maxTotalSize);
    return new SparkExecutorCache(conf);
  }
}
