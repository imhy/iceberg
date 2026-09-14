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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.time.LocalDate;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.io.TaskWriter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

class TestRowDataTypePromotion {
  static Stream<Arguments> modes() {
    return Stream.of(6, 9)
        .flatMap(
            precision ->
                Stream.of(false, true)
                    .flatMap(
                        binary ->
                            Stream.of(RowKind.values())
                                .map(kind -> Arguments.of(precision, binary, kind))));
  }

  @ParameterizedTest
  @MethodSource("modes")
  @SuppressWarnings("unchecked")
  void retainsBufferedRowsAndRowKindWhenInputIsReused(int precision, boolean binary, RowKind kind)
      throws Exception {
    RowType source = rowType(new DateType());
    RowType target = rowType(new TimestampType(precision));
    GenericRowData generic =
        GenericRowData.of(
            1,
            1000L,
            true,
            GenericRowData.of(-1, 2000L),
            new GenericArrayData(new Object[] {1, null}),
            new GenericMapData(Map.of(StringData.fromString("k"), -1)));
    generic.setRowKind(kind);
    RowData input = binary ? new RowDataSerializer(source).toBinaryRow(generic).copy() : generic;
    TaskWriter<RowData> delegate = mock(TaskWriter.class);
    try (TaskWriter<RowData> writer = RowDataTypePromotion.wrap(delegate, source, target)) {
      writer.write(input);
      reuseInput(input);
      writer.write(input);
    }
    ArgumentCaptor<RowData> captured = ArgumentCaptor.forClass(RowData.class);
    verify(delegate, times(2)).write(captured.capture());
    RowData first = captured.getAllValues().get(0);
    RowData second = captured.getAllValues().get(1);
    assertThat(first.getRowKind()).isEqualTo(kind);
    assertThat(first.getTimestamp(0, precision).toLocalDateTime())
        .isEqualTo(LocalDate.parse("1970-01-02").atStartOfDay());
    assertThat(first.getLong(1)).isEqualTo(1000L);
    assertThat(first.getBoolean(2)).isTrue();
    assertThat(first.getRow(3, 2).getTimestamp(0, precision).toLocalDateTime())
        .isEqualTo(LocalDate.parse("1969-12-31").atStartOfDay());
    assertThat(first.getRow(3, 2).getLong(1)).isEqualTo(2000L);
    assertThat(first.getArray(4).getTimestamp(0, precision).toLocalDateTime())
        .isEqualTo(LocalDate.parse("1970-01-02").atStartOfDay());
    assertThat(first.getArray(4).isNullAt(1)).isTrue();
    assertThat(first.getMap(5).keyArray().getString(0).toString()).isEqualTo("k");
    assertThat(first.getMap(5).valueArray().getTimestamp(0, precision).toLocalDateTime())
        .isEqualTo(LocalDate.parse("1969-12-31").atStartOfDay());
    assertThat(second.getRowKind()).isEqualTo(RowKind.DELETE);
    assertThat(second.getTimestamp(0, precision).toLocalDateTime())
        .isEqualTo(LocalDate.parse("1970-01-03").atStartOfDay());
    assertThat(second.getLong(1)).isEqualTo(3000L);
    assertThat(second.getBoolean(2)).isFalse();
    assertThat(second.isNullAt(3)).isTrue();
    assertThat(second.isNullAt(4)).isTrue();
    assertThat(second.isNullAt(5)).isTrue();

    RowData copy = new RowDataSerializer(target).copy(first);
    first.setRowKind(RowKind.INSERT);
    assertThat(copy.getRowKind()).isEqualTo(kind);
    assertThat(input.getRowKind()).isEqualTo(RowKind.DELETE);
    assertThat(copy.getRow(3, 2).getLong(1)).isEqualTo(2000L);
  }

  @ParameterizedTest
  @ValueSource(ints = {6, 9})
  @SuppressWarnings("unchecked")
  void rejectsOverflowBeforePassingRowToWriter(int precision) {
    RowType source = RowType.of(new DateType());
    RowType target = RowType.of(new TimestampType(precision));
    TaskWriter<RowData> delegate = mock(TaskWriter.class);
    TaskWriter<RowData> writer = RowDataTypePromotion.wrap(delegate, source, target);
    assertThatThrownBy(() -> writer.write(GenericRowData.of(Integer.MAX_VALUE)))
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("long overflow");
    verifyNoInteractions(delegate);
  }

  private static RowType rowType(LogicalType date) {
    return RowType.of(
        date,
        new BigIntType(),
        new BooleanType(),
        RowType.of(date, new BigIntType()),
        new ArrayType(date),
        new MapType(new VarCharType(), date));
  }

  private static void reuseInput(RowData row) {
    row.setRowKind(RowKind.DELETE);
    if (row instanceof BinaryRowData binary) {
      binary.setInt(0, 2);
      binary.setLong(1, 3000L);
      binary.setBoolean(2, false);
      binary.setNullAt(3);
      binary.setNullAt(4);
      binary.setNullAt(5);
    } else {
      GenericRowData generic = (GenericRowData) row;
      generic.setField(0, 2);
      generic.setField(1, 3000L);
      generic.setField(2, false);
      generic.setField(3, null);
      generic.setField(4, null);
      generic.setField(5, null);
    }
  }
}
