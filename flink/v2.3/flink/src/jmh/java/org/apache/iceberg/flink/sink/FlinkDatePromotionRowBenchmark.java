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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
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

/** Measures conversion and typed reads from a binary input row, without file IO. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
public class FlinkDatePromotionRowBenchmark {
  @Param({"8", "200"})
  public int fields;

  @Param({"unchanged", "single", "multiple", "nested", "nulls"})
  public String shape;

  private RowData input;
  private TaskWriter<RowData> writer;
  private CapturingWriter capture;
  private boolean[] timestamps;

  @Setup
  public void setup() {
    LogicalType[] from = new LogicalType[fields];
    LogicalType[] to = new LogicalType[fields];
    GenericRowData values = new GenericRowData(fields);
    timestamps = new boolean[fields];
    for (int i = 0; i < fields; i++) {
      boolean promoted =
          !shape.equals("unchanged") && (i == 0 || (shape.equals("multiple") && i % 4 == 0));
      from[i] = promoted ? new DateType() : new BigIntType();
      to[i] = promoted ? new TimestampType(6) : new BigIntType();
      timestamps[i] = promoted;
      values.setField(i, promoted ? Integer.valueOf(18000 + i) : (Object) Long.valueOf(1000L + i));
      if (shape.equals("nested") && i == 0) {
        from[i] = RowType.of(new DateType(), new BigIntType());
        to[i] = RowType.of(new TimestampType(6), new BigIntType());
        values.setField(i, GenericRowData.of(18000, 1000L));
      } else if (shape.equals("nulls") && i % 3 == 0) {
        values.setField(i, null);
      }
    }
    RowType source = RowType.of(from);
    input = new RowDataSerializer(source).toBinaryRow(values).copy();
    capture = new CapturingWriter();
    writer = RowDataTypePromotion.wrap(capture, source, RowType.of(to));
  }

  @Benchmark
  public long convertAndRead() throws IOException {
    writer.write(input);
    RowData row = capture.row;
    long sum = 0;
    for (int i = 0; i < fields; i++) {
      if (!row.isNullAt(i)) {
        if (shape.equals("nested") && i == 0) {
          RowData nested = row.getRow(i, 2);
          sum += nested.getTimestamp(0, 6).getMillisecond() + nested.getLong(1);
        } else if (timestamps[i]) {
          sum += row.getTimestamp(i, 6).getMillisecond();
        } else {
          sum += row.getLong(i);
        }
      }
    }
    return sum;
  }

  private static class CapturingWriter implements TaskWriter<RowData> {
    private RowData row;

    @Override
    public void write(RowData value) {
      row = value;
    }

    @Override
    public void abort() {}

    @Override
    public WriteResult complete() {
      return WriteResult.builder().build();
    }

    @Override
    public void close() {}
  }
}
