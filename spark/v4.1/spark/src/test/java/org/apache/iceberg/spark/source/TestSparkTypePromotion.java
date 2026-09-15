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

import org.apache.iceberg.Schema;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

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
  void convertsBothSupportedLineageLayouts() {
    StructType source =
        new StructType()
            .add("d", DataTypes.DateType)
            .add("_row_id", DataTypes.LongType)
            .add("_last_updated_sequence_number", DataTypes.LongType);
    StructType target =
        new StructType()
            .add("d", DataTypes.TimestampNTZType)
            .add("_row_id", DataTypes.LongType)
            .add("_last_updated_sequence_number", DataTypes.LongType);
    var convert = SparkTypePromotion.rowConverter(source, target);
    InternalRow data = convert.apply(new GenericInternalRow(new Object[] {1}));
    assertThat(data.numFields()).isEqualTo(1);
    assertThat(data.getLong(0)).isEqualTo(86_400_000_000L);
    InternalRow full = convert.apply(new GenericInternalRow(new Object[] {1, 42L, 3L}));
    assertThat(full.numFields()).isEqualTo(3);
    assertThat(full.getLong(0)).isEqualTo(86_400_000_000L);
    assertThat(full.getLong(1)).isEqualTo(42L);
    assertThat(full.getLong(2)).isEqualTo(3L);
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
}
