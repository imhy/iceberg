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
package org.apache.iceberg.flink.sink.shuffle;

import static org.apache.iceberg.flink.sink.shuffle.Fixtures.CHAR_KEYS;
import static org.apache.iceberg.flink.sink.shuffle.Fixtures.SCHEMA;
import static org.apache.iceberg.flink.sink.shuffle.Fixtures.SORT_ORDER;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestRangePartitioner {
  private final int numPartitions = 4;

  @Test
  public void testRoundRobinRecordsBeforeStatisticsAvailable() {
    RangePartitioner partitioner = new RangePartitioner(SCHEMA, SORT_ORDER);
    Set<Integer> results = Sets.newHashSetWithExpectedSize(numPartitions);
    for (int i = 0; i < numPartitions; ++i) {
      results.add(
          partitioner.partition(
              StatisticsOrRecord.fromRecord(GenericRowData.of(StringData.fromString("a"), 1)),
              numPartitions));
    }

    // round-robin. every partition should get an assignment
    assertThat(results).containsExactlyInAnyOrder(0, 1, 2, 3);
  }

  @Test
  public void testRoundRobinStatisticsWrapper() {
    RangePartitioner partitioner = new RangePartitioner(SCHEMA, SORT_ORDER);
    Set<Integer> results = Sets.newHashSetWithExpectedSize(numPartitions);
    for (int i = 0; i < numPartitions; ++i) {
      GlobalStatistics statistics =
          GlobalStatistics.fromRangeBounds(1L, new SortKey[] {CHAR_KEYS.get("a")});
      results.add(
          partitioner.partition(StatisticsOrRecord.fromStatistics(statistics), numPartitions));
    }

    // round-robin. every partition should get an assignment
    assertThat(results).containsExactlyInAnyOrder(0, 1, 2, 3);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void partitionsDateInputUsingTimestampStatistics(boolean binary) {
    Schema schema =
        new Schema(Types.NestedField.required(1, "d", Types.TimestampType.withoutZone()));
    Schema inputSchema = new Schema(Types.NestedField.required(1, "d", Types.DateType.get()));
    RowType inputType = FlinkSchemaUtil.convert(inputSchema);
    SortOrder order = SortOrder.builderFor(schema).asc("d").build();
    SortKey boundary = new SortKey(schema, order);
    boundary.set(0, DateTimeUtil.microsFromDays(1));
    SortKey higher = boundary.copy();
    higher.set(0, DateTimeUtil.microsFromDays(2));
    RowData row = GenericRowData.of(2);
    if (binary) {
      row = new RowDataSerializer(inputType).toBinaryRow(row);
    }
    RangePartitioner sketch = new RangePartitioner(schema, inputType, order);
    sketch.partition(
        StatisticsOrRecord.fromStatistics(
            GlobalStatistics.fromRangeBounds(1L, new SortKey[] {boundary})),
        2);
    assertThat(sketch.partition(StatisticsOrRecord.fromRecord(row), 2)).isEqualTo(1);

    MapAssignment assignment =
        MapAssignment.fromKeyFrequency(
            2,
            Map.of(boundary, 10L, higher, 10L),
            0.0,
            Comparators.forType(SortKeyUtil.sortKeySchema(schema, order).asStruct()));
    RangePartitioner map = new RangePartitioner(schema, inputType, order);
    map.partition(
        StatisticsOrRecord.fromStatistics(GlobalStatistics.fromMapAssignment(1L, assignment)), 2);
    assertThat(map.partition(StatisticsOrRecord.fromRecord(row), 2))
        .isEqualTo(
            new MapRangePartitioner(schema, order, assignment)
                .partition(GenericRowData.of(TimestampData.fromEpochMillis(2 * 86_400_000L)), 2));
  }
}
