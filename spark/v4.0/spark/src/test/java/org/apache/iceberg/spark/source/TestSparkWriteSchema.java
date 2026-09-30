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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestSparkWriteSchema {
  @Test
  void preservesAttributesAndRestoresCurrentTableDefaults() {
    Types.NestedField date =
        Types.NestedField.from(Types.NestedField.required(1, "d", Types.DateType.get(), "date doc"))
            .withInitialDefault(Literal.of(1).to(Types.DateType.get()))
            .withWriteDefault(Literal.of(2).to(Types.DateType.get()))
            .build();
    Schema input = new Schema(17, List.of(date), Map.of("alias", 1), Set.of(1));
    Types.NestedField target = timestampField(1, "d");
    Schema result = SparkWriteSchema.promote(input, new Schema(target));
    assertThat(result.schemaId()).isEqualTo(17);
    assertThat(result.getAliases()).isEqualTo(input.getAliases());
    assertThat(result.identifierFieldIds()).containsExactly(1);
    assertThat(result.findField(1).doc()).isEqualTo("date doc");
    assertThat(result.findField(1).isRequired()).isTrue();
    assertThat(result.findField(1).type()).isEqualTo(Types.TimestampType.withoutZone());
    assertThat(result.findField(1).initialDefault()).isEqualTo(DateTimeUtil.microsFromDays(1));
    assertThat(result.findField(1).writeDefault()).isEqualTo(DateTimeUtil.microsFromDays(3));
    assertThat(input.findField(1).type()).isEqualTo(Types.DateType.get());
  }

  @ParameterizedTest
  @ValueSource(strings = {"struct", "list", "map"})
  void promotesNestedProjectionDefaults(String container) {
    Schema input = nested(container, false);
    Schema result = SparkWriteSchema.promote(input, nested(container, true));
    assertThat(result.findType(4)).isEqualTo(Types.TimestampType.withoutZone());
    assertThat(result.findField(4).initialDefault()).isEqualTo(DateTimeUtil.microsFromDays(1));
    assertThat(result.findField(4).writeDefault()).isEqualTo(DateTimeUtil.microsFromDays(3));
    assertThat(result.findField(5)).isNull();
  }

  @Test
  void promotesPrimitiveCollectionElements() {
    Schema input =
        new Schema(
            Types.NestedField.optional(
                1, "dates", Types.ListType.ofOptional(2, Types.DateType.get())),
            Types.NestedField.optional(
                3,
                "values",
                Types.MapType.ofOptional(4, 5, Types.StringType.get(), Types.DateType.get())));
    Schema target =
        new Schema(
            Types.NestedField.optional(
                1, "dates", Types.ListType.ofOptional(2, Types.TimestampType.withoutZone())),
            Types.NestedField.optional(
                3,
                "values",
                Types.MapType.ofOptional(
                    4, 5, Types.StringType.get(), Types.TimestampType.withoutZone())));
    Schema result = SparkWriteSchema.promote(input, target);
    assertThat(result.asStruct()).isEqualTo(target.asStruct());
  }

  @Test
  void preservesLineageAndUnchangedPrimitiveTypes() {
    Types.NestedField date = Types.NestedField.optional(1, "d", Types.DateType.get());
    Types.NestedField uuid = Types.NestedField.optional(2, "uuid", Types.UUIDType.get());
    Types.NestedField fixed = Types.NestedField.optional(3, "fixed", Types.FixedType.ofLength(4));
    Schema input = MetadataColumns.schemaWithRowLineage(new Schema(date, uuid, fixed));
    Schema target = new Schema(timestampField(1, "d"), uuid, fixed);
    Schema result = SparkWriteSchema.promote(input, target);
    assertThat(result.findField(2)).isEqualTo(uuid);
    assertThat(result.findField(3)).isEqualTo(fixed);
    assertThat(result.findField(MetadataColumns.ROW_ID.fieldId()))
        .isEqualTo(MetadataColumns.ROW_ID);
    assertThat(result.findField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId()))
        .isEqualTo(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);
    assertThat(result.findField(1).writeDefault()).isEqualTo(DateTimeUtil.microsFromDays(3));
  }

  @Test
  void retainsAbsentTableDefaults() {
    Schema input = new Schema(Types.NestedField.optional(1, "d", Types.DateType.get()));
    Schema target =
        new Schema(Types.NestedField.optional(1, "d", Types.TimestampType.withoutZone()));
    Schema result = SparkWriteSchema.promote(input, target);
    assertThat(result.findField(1).initialDefaultLiteral()).isNull();
    assertThat(result.findField(1).writeDefaultLiteral()).isNull();
  }

  private static Types.NestedField timestampField(int id, String name) {
    return Types.NestedField.from(
            Types.NestedField.optional(id, name, Types.TimestampType.withoutZone()))
        .withInitialDefault(
            Literal.of(DateTimeUtil.microsFromDays(1)).to(Types.TimestampType.withoutZone()))
        .withWriteDefault(
            Literal.of(DateTimeUtil.microsFromDays(3)).to(Types.TimestampType.withoutZone()))
        .build();
  }

  private static Schema nested(String container, boolean promoted) {
    Types.NestedField value =
        promoted
            ? timestampField(4, "d")
            : Types.NestedField.optional(4, "d", Types.DateType.get());
    Types.StructType struct =
        promoted
            ? Types.StructType.of(
                value, Types.NestedField.optional(5, "extra", Types.StringType.get()))
            : Types.StructType.of(value);
    Type type =
        switch (container) {
          case "list" -> Types.ListType.ofOptional(2, struct);
          case "map" -> Types.MapType.ofOptional(2, 3, Types.StringType.get(), struct);
          default -> struct;
        };
    return new Schema(Types.NestedField.optional(1, "root", type));
  }
}
