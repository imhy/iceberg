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
package org.apache.iceberg.util;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
public class DatePromotionBenchmark {
  @Param({"MICROS", "NANOS"})
  public ChronoUnit unit;

  private int day;

  private int nextDay() {
    day = day == 100_000 ? -100_000 : day + 1;
    return day;
  }

  @Benchmark
  public long calendarArithmetic() {
    return unit.between(
        DateTimeUtil.EPOCH.toLocalDateTime(), DateTimeUtil.dateFromDays(nextDay()).atStartOfDay());
  }

  @Benchmark
  public long checkedMultiplication() {
    int days = nextDay();
    return unit == ChronoUnit.NANOS
        ? DateTimeUtil.nanosFromDays(days)
        : DateTimeUtil.microsFromDays(days);
  }
}
