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
package org.apache.iceberg.types;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestDateToTimestampConversions {
  private static final long MICROS_PER_DAY = 86_400_000_000L;
  private static final long NANOS_PER_DAY = 86_400_000_000_000L;

  static Stream<Type.PrimitiveType> timestampTypes() {
    return Stream.of(Types.TimestampType.withoutZone(), Types.TimestampNanoType.withoutZone());
  }

  static Stream<Type.PrimitiveType> allTimestampTypes() {
    return Stream.of(
        Types.TimestampType.withoutZone(),
        Types.TimestampType.withZone(),
        Types.TimestampNanoType.withoutZone(),
        Types.TimestampNanoType.withZone());
  }

  @Test
  void preservesNumericPromotions() {
    for (int value : new int[] {Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE}) {
      ByteBuffer bound = Conversions.toByteBuffer(Types.IntegerType.get(), value);
      assertThat(Conversions.<Long>fromByteBuffer(Types.LongType.get(), bound))
          .isEqualTo((long) value);
      assertThat(Conversions.<Integer>fromByteBuffer(Types.DateType.get(), bound)).isEqualTo(value);
    }
    for (float value : new float[] {-Float.MAX_VALUE, -1.5F, 0, 1.5F, Float.MAX_VALUE}) {
      ByteBuffer bound = Conversions.toByteBuffer(Types.FloatType.get(), value);
      assertThat(Conversions.<Double>fromByteBuffer(Types.DoubleType.get(), bound))
          .isEqualTo((double) value);
    }
  }

  @ParameterizedTest
  @MethodSource("timestampTypes")
  void historicalDateBounds(Type.PrimitiveType type) {
    long unitsPerDay = type.typeId() == Type.TypeID.TIMESTAMP ? MICROS_PER_DAY : NANOS_PER_DAY;
    int minDay = (int) (Long.MIN_VALUE / unitsPerDay);
    int maxDay = (int) (Long.MAX_VALUE / unitsPerDay);
    for (int day : new int[] {minDay, -1, 0, 1, 20_000, maxDay}) {
      ByteBuffer bound = Conversions.toByteBuffer(Types.DateType.get(), day);
      assertThat(Conversions.<Long>fromByteBuffer(type, bound)).isEqualTo(day * unitsPerDay);
    }
  }

  @ParameterizedTest
  @MethodSource("timestampTypes")
  void overflowingDateBounds(Type.PrimitiveType type) {
    long unitsPerDay = type.typeId() == Type.TypeID.TIMESTAMP ? MICROS_PER_DAY : NANOS_PER_DAY;
    int minDay = (int) (Long.MIN_VALUE / unitsPerDay);
    int maxDay = (int) (Long.MAX_VALUE / unitsPerDay);
    for (int day : new int[] {Integer.MIN_VALUE, minDay - 1, maxDay + 1, Integer.MAX_VALUE}) {
      ByteBuffer bound = Conversions.toByteBuffer(Types.DateType.get(), day);
      assertThatThrownBy(() -> Conversions.fromByteBuffer(type, bound))
          .isInstanceOf(ArithmeticException.class);
    }
  }

  @ParameterizedTest
  @MethodSource("timestampTypes")
  void preservesBufferState(Type.PrimitiveType type) {
    long unitsPerDay = type.typeId() == Type.TypeID.TIMESTAMP ? MICROS_PER_DAY : NANOS_PER_DAY;
    ByteBuffer storage = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
    storage.putInt(4, -1);
    storage.position(4).limit(8);
    for (ByteBuffer bound :
        new ByteBuffer[] {storage, storage.slice(), storage.asReadOnlyBuffer()}) {
      bound.order(ByteOrder.BIG_ENDIAN);
      int position = bound.position();
      int limit = bound.limit();
      assertThat(Conversions.<Long>fromByteBuffer(type, bound)).isEqualTo(-unitsPerDay);
      assertThat(Conversions.<Long>fromByteBuffer(type, bound)).isEqualTo(-unitsPerDay);
      assertThat(bound.position()).isEqualTo(position);
      assertThat(bound.limit()).isEqualTo(limit);
      assertThat(bound.order()).isEqualTo(ByteOrder.BIG_ENDIAN);
    }
  }

  @ParameterizedTest
  @MethodSource("allTimestampTypes")
  void currentTimestampBounds(Type.PrimitiveType type) {
    for (long value : new long[] {Long.MIN_VALUE, -123456789L, 0, 123456789L, Long.MAX_VALUE}) {
      assertThat(Conversions.<Long>fromByteBuffer(type, Conversions.toByteBuffer(type, value)))
          .isEqualTo(value);
    }
    assertThat(Conversions.<Long>fromByteBuffer(type, null)).isNull();
  }

  @ParameterizedTest
  @MethodSource("timestampTypes")
  void rejectsDateBoundsForZonedTimestamps(Type.PrimitiveType type) {
    Type.PrimitiveType zoned =
        type.typeId() == Type.TypeID.TIMESTAMP
            ? Types.TimestampType.withZone()
            : Types.TimestampNanoType.withZone();
    ByteBuffer bound = Conversions.toByteBuffer(Types.DateType.get(), 1);
    assertThatThrownBy(() -> Conversions.fromByteBuffer(zoned, bound))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
