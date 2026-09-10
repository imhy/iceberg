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
package org.apache.iceberg.expressions;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestDateToTimestampMetrics {
  static Stream<Type.PrimitiveType> timestampTypes() {
    return Stream.of(Types.TimestampType.withoutZone(), Types.TimestampNanoType.withoutZone());
  }

  @ParameterizedTest
  @MethodSource("timestampTypes")
  void historicalBounds(Type.PrimitiveType type) {
    Schema schema = schema(type);
    for (int day : new int[] {-1, 0, 1, 20_000}) {
      String midnight = timestamp(day);
      String before = timestamp(day - 1);
      String after = timestamp(day + 1);
      DataFile file = dateFile(day, day);
      for (Expression expr :
          List.of(
              Expressions.equal("d", midnight),
              Expressions.lessThan("d", after),
              Expressions.lessThanOrEqual("d", midnight),
              Expressions.greaterThan("d", before),
              Expressions.greaterThanOrEqual("d", midnight),
              Expressions.in("d", before, midnight),
              Expressions.notEqual("d", after),
              Expressions.notIn("d", before, after))) {
        assertThat(new InclusiveMetricsEvaluator(schema, expr).eval(file))
            .as("inclusive %s", expr)
            .isTrue();
        assertThat(new StrictMetricsEvaluator(schema, expr).eval(file))
            .as("strict %s", expr)
            .isTrue();
      }
      for (Expression expr :
          List.of(
              Expressions.equal("d", before),
              Expressions.equal("d", after),
              Expressions.lessThan("d", midnight),
              Expressions.lessThanOrEqual("d", before),
              Expressions.greaterThan("d", midnight),
              Expressions.greaterThanOrEqual("d", after),
              Expressions.in("d", before, after))) {
        assertThat(new InclusiveMetricsEvaluator(schema, expr).eval(file))
            .as("inclusive %s", expr)
            .isFalse();
        assertThat(new StrictMetricsEvaluator(schema, expr).eval(file))
            .as("strict %s", expr)
            .isFalse();
      }
      for (Expression expr :
          List.of(Expressions.notEqual("d", midnight), Expressions.notIn("d", midnight))) {
        assertThat(new StrictMetricsEvaluator(schema, expr).eval(file)).isFalse();
      }
    }
  }

  @ParameterizedTest
  @MethodSource("timestampTypes")
  void rangesDoNotProveEveryRowMatches(Type.PrimitiveType type) {
    Schema schema = schema(type);
    DataFile file = dateFile(-1, 1);
    for (Expression expr :
        List.of(
            Expressions.equal("d", timestamp(0)),
            Expressions.lessThan("d", timestamp(0)),
            Expressions.greaterThan("d", timestamp(0)),
            Expressions.in("d", timestamp(0)),
            Expressions.notEqual("d", timestamp(0)),
            Expressions.notIn("d", timestamp(0)))) {
      assertThat(new InclusiveMetricsEvaluator(schema, expr).eval(file)).isTrue();
      assertThat(new StrictMetricsEvaluator(schema, expr).eval(file)).isFalse();
    }
  }

  @ParameterizedTest
  @MethodSource("timestampTypes")
  void missingAndNullBounds(Type.PrimitiveType type) {
    Schema schema = schema(type);
    Expression equality = Expressions.equal("d", timestamp(1));
    Map<Integer, ByteBuffer> bound = Map.of(1, Conversions.toByteBuffer(Types.DateType.get(), 1));
    for (DataFile file :
        List.of(
            file(1, 0, null, null),
            file(1, 0, Map.of(), Map.of()),
            file(1, 0, bound, null),
            file(1, 0, null, bound))) {
      assertThat(new InclusiveMetricsEvaluator(schema, equality).eval(file)).isTrue();
      assertThat(new StrictMetricsEvaluator(schema, equality).eval(file)).isFalse();
    }
    DataFile nulls = file(1, 1, null, null);
    assertThat(new InclusiveMetricsEvaluator(schema, equality).eval(nulls)).isFalse();
    assertThat(new StrictMetricsEvaluator(schema, equality).eval(nulls)).isFalse();
    assertThat(new InclusiveMetricsEvaluator(schema, Expressions.isNull("d")).eval(nulls)).isTrue();
    assertThat(new StrictMetricsEvaluator(schema, Expressions.isNull("d")).eval(nulls)).isTrue();
    DataFile empty = file(0, 0, null, null);
    assertThat(new InclusiveMetricsEvaluator(schema, equality).eval(empty)).isFalse();
    assertThat(new StrictMetricsEvaluator(schema, equality).eval(empty)).isTrue();
  }

  @ParameterizedTest
  @MethodSource("timestampTypes")
  void aggregatesHistoricalAndCurrentBounds(Type.PrimitiveType type) {
    AggregateEvaluator evaluator =
        AggregateEvaluator.create(
            schema(type), List.of(Expressions.min("d"), Expressions.max("d")));
    evaluator.update(dateFile(-1, 1));
    assertThat(evaluator.result().get(0, Long.class)).isEqualTo(-unitsPerDay(type));
    assertThat(evaluator.result().get(1, Long.class)).isEqualTo(unitsPerDay(type));
    long currentValue = 2 * unitsPerDay(type) + 123;
    Map<Integer, ByteBuffer> currentBound = Map.of(1, Conversions.toByteBuffer(type, currentValue));
    evaluator.update(file(1, 0, currentBound, currentBound));
    evaluator.update(file(1, 1, null, null));
    assertThat(evaluator.allAggregatorsValid()).isTrue();
    assertThat(evaluator.result().get(0, Long.class)).isEqualTo(-unitsPerDay(type));
    assertThat(evaluator.result().get(1, Long.class)).isEqualTo(currentValue);
    evaluator.update(file(1, 0, null, null));
    assertThat(evaluator.allAggregatorsValid()).isFalse();
  }

  @ParameterizedTest
  @MethodSource("timestampTypes")
  void overflowingBoundsAreSaturated(Type.PrimitiveType type) {
    Schema schema = schema(type);
    Expression equality = Expressions.equal("d", timestamp(1));
    // bounds below the representable range saturate to Long.MIN_VALUE and cannot match an
    // in-range literal
    int minDay = (int) (Long.MIN_VALUE / unitsPerDay(type)) - 1;
    DataFile below = dateFile(minDay, minDay);
    assertThat(new InclusiveMetricsEvaluator(schema, equality).eval(below)).isFalse();
    assertThat(new StrictMetricsEvaluator(schema, equality).eval(below)).isFalse();
    AggregateEvaluator belowAggregates =
        AggregateEvaluator.create(schema, List.of(Expressions.min("d"), Expressions.max("d")));
    belowAggregates.update(below);
    assertThat(belowAggregates.result().get(0, Long.class)).isEqualTo(Long.MIN_VALUE);
    assertThat(belowAggregates.result().get(1, Long.class)).isEqualTo(Long.MIN_VALUE);
    // bounds above the representable range saturate to Long.MAX_VALUE
    int maxDay = (int) (Long.MAX_VALUE / unitsPerDay(type)) + 1;
    DataFile above = dateFile(maxDay, maxDay);
    assertThat(new InclusiveMetricsEvaluator(schema, equality).eval(above)).isFalse();
    assertThat(new StrictMetricsEvaluator(schema, equality).eval(above)).isFalse();
    AggregateEvaluator aboveAggregates =
        AggregateEvaluator.create(schema, List.of(Expressions.min("d"), Expressions.max("d")));
    aboveAggregates.update(above);
    assertThat(aboveAggregates.result().get(0, Long.class)).isEqualTo(Long.MAX_VALUE);
    assertThat(aboveAggregates.result().get(1, Long.class)).isEqualTo(Long.MAX_VALUE);
    // conservative bounds enclose the literal after saturation, so the file is not pruned
    DataFile enclosing = dateFile(minDay, maxDay);
    assertThat(new InclusiveMetricsEvaluator(schema, equality).eval(enclosing)).isTrue();
    assertThat(new StrictMetricsEvaluator(schema, equality).eval(enclosing)).isFalse();
  }

  private static Schema schema(Type.PrimitiveType type) {
    return new Schema(Types.NestedField.optional(1, "d", type));
  }

  private static String timestamp(int day) {
    return LocalDate.ofEpochDay(day).atStartOfDay().toString();
  }

  private static long unitsPerDay(Type type) {
    return type.typeId() == Type.TypeID.TIMESTAMP ? 86_400_000_000L : 86_400_000_000_000L;
  }

  private static DataFile dateFile(int lower, int upper) {
    return file(
        2,
        0,
        Map.of(1, Conversions.toByteBuffer(Types.DateType.get(), lower)),
        Map.of(1, Conversions.toByteBuffer(Types.DateType.get(), upper)));
  }

  private static DataFile file(
      long count, long nulls, Map<Integer, ByteBuffer> lower, Map<Integer, ByteBuffer> upper) {
    return new TestDataFile(
        "date.parquet", Row.of(), count, Map.of(1, count), Map.of(1, nulls), null, lower, upper);
  }
}
