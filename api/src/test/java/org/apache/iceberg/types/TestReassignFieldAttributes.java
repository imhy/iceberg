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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestReassignFieldAttributes {
  @Test
  void preservesProjectionIdentityAndCopiesDistinctDefaults() {
    Types.NestedField source =
        Types.NestedField.from(
                Types.NestedField.required(1, "id", Types.IntegerType.get(), "source doc"))
            .withInitialDefault(Literal.of(10))
            .withWriteDefault(Literal.of(20))
            .build();
    Schema projection =
        new Schema(
            17,
            List.of(
                Types.NestedField.required(
                    1, "renamed", Types.IntegerType.get(), "projection doc")),
            Map.of("alias", 1),
            Set.of(1));
    Schema result = TypeUtil.reassignDefaults(projection, new Schema(source));
    assertThat(result.schemaId()).isEqualTo(17);
    assertThat(result.getAliases()).containsExactlyEntriesOf(projection.getAliases());
    assertThat(result.identifierFieldIds()).containsExactly(1);
    assertThat(result.findField(1).name()).isEqualTo("renamed");
    assertThat(result.findField(1).isRequired()).isTrue();
    assertThat(result.findField(1).doc()).isEqualTo("projection doc");
    assertThat(result.findField(1).initialDefault()).isEqualTo(10);
    assertThat(result.findField(1).writeDefault()).isEqualTo(20);
    Schema withDoc = TypeUtil.reassignDoc(result, new Schema(source));
    assertThat(withDoc.schemaId()).isEqualTo(result.schemaId());
    assertThat(withDoc.getAliases()).isEqualTo(result.getAliases());
    assertThat(withDoc.identifierFieldIds()).isEqualTo(result.identifierFieldIds());
    assertThat(withDoc.findField(1).doc()).isEqualTo("source doc");
    assertThat(withDoc.findField(1).writeDefault()).isEqualTo(20);
  }

  @ParameterizedTest
  @ValueSource(strings = {"struct", "list", "map"})
  void restoresDefaultsInNestedProjections(String container) {
    Schema projection = nested(container, false);
    Schema result = TypeUtil.reassignDefaults(projection, nested(container, true));
    assertThat(result.findField(4).initialDefault()).isEqualTo(10);
    assertThat(result.findField(4).writeDefault()).isEqualTo(20);
    assertThat(result.findField(5)).isNull();
    assertThat(result.findField(4).isOptional()).isTrue();
  }

  @Test
  void doesNotCopyTimestampDefaultsIntoDateFields() {
    Types.NestedField date =
        Types.NestedField.from(Types.NestedField.optional(1, "d", Types.DateType.get()))
            .withInitialDefault(Literal.of(1).to(Types.DateType.get()))
            .build();
    Types.NestedField timestamp =
        Types.NestedField.from(
                Types.NestedField.optional(1, "d", Types.TimestampType.withoutZone()))
            .withInitialDefault(Literal.of(86_400_000_000L).to(Types.TimestampType.withoutZone()))
            .build();
    Schema result = TypeUtil.reassignDefaults(new Schema(date), new Schema(timestamp));
    assertThat(result.findField(1).type()).isEqualTo(Types.DateType.get());
    assertThat(result.findField(1).initialDefault()).isEqualTo(1);
  }

  @Test
  void restoresAbsentDefaults() {
    Types.NestedField plain = Types.NestedField.optional(1, "v", Types.IntegerType.get());
    Types.NestedField withDefault =
        Types.NestedField.from(plain)
            .withInitialDefault(Literal.of(1))
            .withWriteDefault(Literal.of(2))
            .build();
    Schema result = TypeUtil.reassignDefaults(new Schema(withDefault), new Schema(plain));
    assertThat(result.findField(1).initialDefaultLiteral()).isNull();
    assertThat(result.findField(1).writeDefaultLiteral()).isNull();
  }

  @Test
  void rejectsMissingSourceField() {
    Schema projection = new Schema(Types.NestedField.optional(1, "v", Types.IntegerType.get()));
    assertThatThrownBy(() -> TypeUtil.reassignDefaults(projection, new Schema()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Field 1 not found in source schema");
  }

  private static Schema nested(String container, boolean defaults) {
    Types.NestedField value = Types.NestedField.optional(4, "v", Types.IntegerType.get());
    if (defaults) {
      value =
          Types.NestedField.from(value)
              .withInitialDefault(Literal.of(10))
              .withWriteDefault(Literal.of(20))
              .build();
    }
    Types.StructType struct =
        defaults
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
