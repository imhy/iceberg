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
package org.apache.iceberg.flink.sink.dynamic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestDateToTimestampConversion {
  static Stream<Type.PrimitiveType> targets() {
    return Stream.of(Types.TimestampType.withoutZone(), Types.TimestampNanoType.withoutZone());
  }

  @ParameterizedTest
  @MethodSource("targets")
  void recognizesDateInput(Type.PrimitiveType target) {
    assertThat(CompareSchemasVisitor.isDataConversionPossible(Types.DateType.get(), target))
        .isTrue();
    assertThat(
            CompareSchemasVisitor.visit(schema(Types.DateType.get()), schema(target), true, false))
        .isEqualTo(CompareSchemasVisitor.Result.DATA_CONVERSION_NEEDED);
  }

  @ParameterizedTest
  @MethodSource("targets")
  void convertsMidnightWithinTargetRange(Type.PrimitiveType target) {
    DataConverter converter = converter(schema(Types.DateType.get()), schema(target));
    for (Integer days :
        Arrays.asList(
            (int) (Long.MIN_VALUE / unitsPerDay(target)),
            -1,
            0,
            1,
            (int) (Long.MAX_VALUE / unitsPerDay(target)),
            null)) {
      Object expected =
          days == null
              ? null
              : TimestampData.fromLocalDateTime(LocalDate.ofEpochDay(days).atStartOfDay());
      assertThat(converter.convert(GenericRowData.of(days))).isEqualTo(GenericRowData.of(expected));
    }
  }

  @ParameterizedTest
  @MethodSource("targets")
  void rejectsOverflow(Type.PrimitiveType target) {
    DataConverter converter = converter(schema(Types.DateType.get()), schema(target));
    for (int days :
        new int[] {
          (int) (Long.MIN_VALUE / unitsPerDay(target)) - 1,
          (int) (Long.MAX_VALUE / unitsPerDay(target)) + 1,
          Integer.MIN_VALUE,
          Integer.MAX_VALUE
        }) {
      assertThatThrownBy(() -> converter.convert(GenericRowData.of(days)))
          .isInstanceOf(ArithmeticException.class);
    }
  }

  @ParameterizedTest
  @MethodSource("targets")
  void convertsNestedValuesAndChecksTheirRange(Type.PrimitiveType target) {
    for (Type.TypeID container :
        new Type.TypeID[] {Type.TypeID.STRUCT, Type.TypeID.LIST, Type.TypeID.MAP}) {
      DataConverter converter =
          converter(
              schema(nestedType(container, Types.DateType.get())),
              schema(nestedType(container, target)));
      Object midnight = TimestampData.fromLocalDateTime(LocalDate.ofEpochDay(-1).atStartOfDay());
      RowData converted =
          (RowData) converter.convert(GenericRowData.of(nestedValue(container, -1)));
      Object actual =
          switch (container) {
            case STRUCT -> converted.getRow(0, 1).getTimestamp(0, precision(target));
            case LIST -> converted.getArray(0).getTimestamp(0, precision(target));
            case MAP -> converted.getMap(0).valueArray().getTimestamp(0, precision(target));
            default -> throw new IllegalArgumentException("Unexpected container: " + container);
          };
      assertThat(actual).isEqualTo(midnight);
      assertThat(converter.convert(GenericRowData.of((Object) null)))
          .isEqualTo(GenericRowData.of((Object) null));
      int overflow = (int) (Long.MAX_VALUE / unitsPerDay(target)) + 1;
      assertThatThrownBy(
              () -> converter.convert(GenericRowData.of(nestedValue(container, overflow))))
          .isInstanceOf(ArithmeticException.class);
    }
  }

  @ParameterizedTest
  @MethodSource("targets")
  void preservesExistingTimestampValues(Type.PrimitiveType target) {
    TimestampData value = TimestampData.fromEpochMillis(-1, 123000);
    assertThat(converter(schema(target), schema(target)).convert(GenericRowData.of(value)))
        .isEqualTo(GenericRowData.of(value));
  }

  private static Schema schema(Type type) {
    return new Schema(Types.NestedField.optional(1, "v", type));
  }

  private static DataConverter converter(Schema source, Schema target) {
    return DataConverter.get(FlinkSchemaUtil.convert(source), FlinkSchemaUtil.convert(target));
  }

  private static int precision(Type type) {
    return type instanceof Types.TimestampNanoType ? 9 : 6;
  }

  private static long unitsPerDay(Type type) {
    return type instanceof Types.TimestampNanoType ? 86_400_000_000_000L : 86_400_000_000L;
  }

  private static Type nestedType(Type.TypeID container, Type element) {
    return switch (container) {
      case STRUCT -> Types.StructType.of(Types.NestedField.optional(2, "d", element));
      case LIST -> Types.ListType.ofOptional(2, element);
      case MAP -> Types.MapType.ofOptional(2, 3, Types.IntegerType.get(), element);
      default -> throw new IllegalArgumentException("Unexpected container: " + container);
    };
  }

  private static Object nestedValue(Type.TypeID container, int days) {
    return switch (container) {
      case STRUCT -> GenericRowData.of(days);
      case LIST -> new GenericArrayData(new Integer[] {days, null});
      case MAP -> new GenericMapData(Collections.singletonMap(7, days));
      default -> throw new IllegalArgumentException("Unexpected container: " + container);
    };
  }
}
