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

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.iceberg.common.DynMethods;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
public class SparkDatePromotionRowBenchmark {
  @Param({"8", "200"})
  public int fields;

  @Param({"unchanged", "single", "multiple", "nested", "nulls"})
  public String shape;

  private InternalRow input;
  private Function<InternalRow, InternalRow> convert;

  @Setup
  public void setup() {
    StructField[] from = new StructField[fields];
    StructField[] to = new StructField[fields];
    Object[] values = new Object[fields];
    for (int i = 0; i < fields; i++) {
      boolean promoted =
          !shape.equals("unchanged") && (i == 0 || (shape.equals("multiple") && i % 4 == 0));
      DataType source = promoted ? DataTypes.DateType : DataTypes.LongType;
      DataType target = promoted ? DataTypes.TimestampNTZType : DataTypes.LongType;
      // Keep boxed setup values out of the measured path; conversion starts from UnsafeRow.
      values[i] = promoted ? Integer.valueOf(18000 + i) : (Object) Long.valueOf(1000L + i);
      if (shape.equals("nested") && i == 0) {
        source = new StructType().add("d", DataTypes.DateType).add("id", DataTypes.LongType);
        target =
            new StructType().add("d", DataTypes.TimestampNTZType).add("id", DataTypes.LongType);
        values[i] = new GenericInternalRow(new Object[] {18000, 1000L});
      } else if (shape.equals("nulls") && i % 3 == 0) {
        values[i] = null;
      }
      from[i] = DataTypes.createStructField("f" + i, source, true);
      to[i] = DataTypes.createStructField("f" + i, target, true);
    }
    StructType source = new StructType(from);
    UnsafeProjection binary = UnsafeProjection.create(source);
    input =
        ((InternalRow)
                DynMethods.builder("apply")
                    .impl(UnsafeProjection.class, InternalRow.class)
                    .build()
                    .bind(binary)
                    .invoke(new GenericInternalRow(values)))
            .copy();
    convert = SparkTypePromotion.rowConverter(source, new StructType(to));
  }

  @Benchmark
  public long convertAndRead() {
    InternalRow row = convert.apply(input);
    long sum = 0;
    for (int i = 0; i < fields; i++) {
      if (!row.isNullAt(i)) {
        if (shape.equals("nested") && i == 0) {
          InternalRow nested = row.getStruct(i, 2);
          sum += nested.getLong(0) + nested.getLong(1);
        } else {
          sum += row.getLong(i);
        }
      }
    }
    return sum;
  }
}
