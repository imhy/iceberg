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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.apache.iceberg.Schema;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import scala.Function1;

class TestSparkTypePromotion {
  @Test
  void rejectsMismatchedInputSchema() {
    Schema schema = new Schema(Types.NestedField.optional(1, "d", Types.DateType.get()));
    StructType input = SparkSchemaUtil.convert(schema).add("extra", DataTypes.LongType);
    assertThatThrownBy(() -> SparkTypePromotion.writeType(input, schema, schema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Spark input schema does not match Iceberg field count");
  }

  @Test
  void rejectsOversizedRow() {
    StructType source = new StructType().add("d", DataTypes.DateType);
    StructType target = new StructType().add("d", DataTypes.TimestampNTZType);
    assertThatThrownBy(
            () ->
                SparkTypePromotion.rowConverter(source, target)
                    .apply(new GenericInternalRow(new Object[] {1, 2L})))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Input row does not match the schema field count");
  }

  @Test
  void matchesReorderedNestedTargetFieldsById() {
    Types.NestedField date = Types.NestedField.optional(2, "d", Types.DateType.get());
    Types.NestedField id = Types.NestedField.optional(3, "id", Types.IntegerType.get());
    Schema source = new Schema(Types.NestedField.optional(1, "s", Types.StructType.of(date, id)));
    Schema target =
        new Schema(
            Types.NestedField.optional(
                1,
                "s",
                Types.StructType.of(
                    id,
                    Types.NestedField.from(date)
                        .ofType(Types.TimestampType.withoutZone())
                        .build())));
    StructType input = SparkSchemaUtil.convert(source);
    StructType output = SparkTypePromotion.writeType(input, source, target);
    InternalRow row =
        SparkTypePromotion.rowConverter(input, output)
            .apply(
                new GenericInternalRow(new Object[] {new GenericInternalRow(new Object[] {1, 7})}));
    assertThat(row.getStruct(0, 2).getLong(0)).isEqualTo(86_400_000_000L);
    assertThat(row.getStruct(0, 2).getInt(1)).isEqualTo(7);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("unchecked")
  void retainsBufferedRowsWhenInputIsReused(boolean binary) throws Exception {
    StructType nested = new StructType().add("d", DataTypes.DateType).add("id", DataTypes.LongType);
    StructType promotedNested =
        new StructType().add("d", DataTypes.TimestampNTZType).add("id", DataTypes.LongType);
    StructType source =
        new StructType()
            .add("d", DataTypes.DateType)
            .add("id", DataTypes.LongType)
            .add("flag", DataTypes.BooleanType)
            .add("s", nested)
            .add("a", DataTypes.createArrayType(DataTypes.DateType))
            .add("m", DataTypes.createMapType(DataTypes.StringType, DataTypes.DateType));
    StructType target =
        new StructType()
            .add("d", DataTypes.TimestampNTZType)
            .add("id", DataTypes.LongType)
            .add("flag", DataTypes.BooleanType)
            .add("s", promotedNested)
            .add("a", DataTypes.createArrayType(DataTypes.TimestampNTZType))
            .add("m", DataTypes.createMapType(DataTypes.StringType, DataTypes.TimestampNTZType));
    GenericInternalRow generic =
        new GenericInternalRow(
            new Object[] {
              1,
              1000L,
              true,
              new GenericInternalRow(new Object[] {-1, 2000L}),
              new GenericArrayData(new Object[] {1, null}),
              new ArrayBasedMapData(
                  new GenericArrayData(new Object[] {UTF8String.fromString("k")}),
                  new GenericArrayData(new Object[] {-1}))
            });
    Function1<InternalRow, InternalRow> toBinary = UnsafeProjection.create(source);
    InternalRow input = binary ? toBinary.apply(generic).copy() : generic;
    DataWriter<InternalRow> delegate = mock(DataWriter.class);
    try (DataWriter<InternalRow> writer = SparkTypePromotion.wrap(delegate, source, target)) {
      writer.write(input);
      input.setInt(0, 2);
      input.setLong(1, 3000L);
      input.setBoolean(2, false);
      input.setNullAt(3);
      input.setNullAt(4);
      input.setNullAt(5);
      writer.write(input);
    }
    ArgumentCaptor<InternalRow> captured = ArgumentCaptor.forClass(InternalRow.class);
    verify(delegate, times(2)).write(captured.capture());
    InternalRow first = captured.getAllValues().get(0);
    InternalRow second = captured.getAllValues().get(1);
    assertThat(first.getLong(0)).isEqualTo(86_400_000_000L);
    assertThat(first.getLong(1)).isEqualTo(1000L);
    assertThat(first.getBoolean(2)).isTrue();
    assertThat(first.getStruct(3, 2).getLong(0)).isEqualTo(-86_400_000_000L);
    assertThat(first.getStruct(3, 2).getLong(1)).isEqualTo(2000L);
    assertThat(first.getArray(4).getLong(0)).isEqualTo(86_400_000_000L);
    assertThat(first.getArray(4).isNullAt(1)).isTrue();
    assertThat(first.getMap(5).keyArray().getUTF8String(0).toString()).isEqualTo("k");
    assertThat(first.getMap(5).valueArray().getLong(0)).isEqualTo(-86_400_000_000L);
    assertThat(second.getLong(0)).isEqualTo(172_800_000_000L);
    assertThat(second.getLong(1)).isEqualTo(3000L);
    assertThat(second.getBoolean(2)).isFalse();
    assertThat(second.isNullAt(3)).isTrue();
    assertThat(second.isNullAt(4)).isTrue();
    assertThat(second.isNullAt(5)).isTrue();

    InternalRow copy = first.copy();
    copy.setLong(1, -1L);
    copy.getStruct(3, 2).setLong(1, -1L);
    assertThat(first.getLong(1)).isEqualTo(1000L);
    assertThat(first.getStruct(3, 2).getLong(1)).isEqualTo(2000L);
  }

  @Test
  @SuppressWarnings("unchecked")
  void rejectsOverflowBeforePassingRowToWriter() {
    StructType source = new StructType().add("d", DataTypes.DateType);
    StructType target = new StructType().add("d", DataTypes.TimestampNTZType);
    DataWriter<InternalRow> delegate = mock(DataWriter.class);
    DataWriter<InternalRow> writer = SparkTypePromotion.wrap(delegate, source, target);
    assertThatThrownBy(() -> writer.write(new GenericInternalRow(new Object[] {Integer.MAX_VALUE})))
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("long overflow");
    verifyNoInteractions(delegate);
  }
}
