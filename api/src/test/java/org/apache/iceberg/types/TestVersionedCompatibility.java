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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestVersionedCompatibility {
  static Stream<Arguments> promotions() {
    return Stream.of(1, 2, 3, 4)
        .flatMap(
            version ->
                Stream.of(Types.TimestampType.withoutZone(), Types.TimestampNanoType.withoutZone())
                    .flatMap(
                        target ->
                            Stream.of(false, true)
                                .map(nested -> Arguments.of(version, target, nested))));
  }

  @ParameterizedTest
  @MethodSource("promotions")
  void usesFormatVersionAtEveryEntryPoint(int version, Type.PrimitiveType target, boolean nested) {
    Schema read = schema(target, nested);
    Schema write = schema(Types.DateType.get(), nested);
    List<List<String>> errors =
        List.of(
            CheckCompatibility.readCompatibilityErrors(version, read, write),
            CheckCompatibility.writeCompatibilityErrors(version, read, write),
            CheckCompatibility.writeCompatibilityErrors(version, read, write, false),
            CheckCompatibility.typeCompatibilityErrors(version, read, write),
            CheckCompatibility.typeCompatibilityErrors(version, read, write, false));
    if (version >= 3) {
      assertThat(errors).allSatisfy(e -> assertThat(e).isEmpty());
      assertThatCode(() -> TypeUtil.validateWriteSchema(version, read, write, true, true))
          .doesNotThrowAnyException();
      assertThatCode(() -> TypeUtil.validateWriteSchema(version, read, write, false, false))
          .doesNotThrowAnyException();
      assertThatCode(() -> TypeUtil.validateSchema(version, "row", read, write, true, true))
          .doesNotThrowAnyException();
      assertThatCode(() -> TypeUtil.validateSchema(version, "row", read, write, false, false))
          .doesNotThrowAnyException();
    } else {
      assertThat(errors).allSatisfy(e -> assertThat(e).isNotEmpty());
      for (boolean nullability : new boolean[] {false, true}) {
        assertThatThrownBy(
                () -> TypeUtil.validateWriteSchema(version, read, write, nullability, true))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cannot be promoted");
        assertThatThrownBy(
                () -> TypeUtil.validateSchema(version, "row", read, write, nullability, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cannot be promoted");
      }
    }
    assertThat(CheckCompatibility.readCompatibilityErrors(version, write, read)).isNotEmpty();
  }

  static Stream<Arguments> legacyPromotions() {
    return Stream.of(Types.TimestampType.withoutZone(), Types.TimestampNanoType.withoutZone())
        .flatMap(target -> Stream.of(false, true).map(nested -> Arguments.of(target, nested)));
  }

  @ParameterizedTest
  @MethodSource("legacyPromotions")
  void keepsLegacyWrappers(Type.PrimitiveType target, boolean nested) {
    Schema read = schema(target, nested);
    Schema write = schema(Types.DateType.get(), nested);
    assertThat(CheckCompatibility.readCompatibilityErrors(read, write)).isNotEmpty();
    assertThat(CheckCompatibility.writeCompatibilityErrors(read, write)).isNotEmpty();
    assertThat(CheckCompatibility.writeCompatibilityErrors(read, write, false)).isNotEmpty();
    assertThat(CheckCompatibility.typeCompatibilityErrors(read, write)).isNotEmpty();
    assertThat(CheckCompatibility.typeCompatibilityErrors(read, write, false)).isNotEmpty();
    assertThatThrownBy(() -> TypeUtil.validateWriteSchema(read, write, true, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be promoted");
    assertThatThrownBy(() -> TypeUtil.validateSchema("row", read, write, false, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be promoted");
    assertThat(TypeUtil.isPromotionAllowed(Types.DateType.get(), target)).isFalse();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4})
  void preservesOrdinaryPromotionsAndValidation(int version) {
    Schema read = new Schema(Types.NestedField.required(1, "n", Types.LongType.get()));
    Schema write = new Schema(Types.NestedField.optional(1, "n", Types.IntegerType.get()));
    assertThat(CheckCompatibility.readCompatibilityErrors(version, read, write))
        .containsExactly("n should be required, but is optional");
    assertThat(CheckCompatibility.writeCompatibilityErrors(version, read, write))
        .containsExactly("n should be required, but is optional");
    assertThat(CheckCompatibility.typeCompatibilityErrors(version, read, write)).isEmpty();
    assertThatCode(() -> TypeUtil.validateWriteSchema(version, read, write, false, true))
        .doesNotThrowAnyException();
    assertThat(CheckCompatibility.typeCompatibilityErrors(read, write)).isEmpty();

    Schema ordered =
        new Schema(
            Types.NestedField.optional(1, "a", Types.LongType.get()),
            Types.NestedField.optional(2, "b", Types.StringType.get()));
    Schema reversed = new Schema(ordered.findField("b"), ordered.findField("a"));
    assertThat(CheckCompatibility.writeCompatibilityErrors(version, ordered, reversed))
        .isNotEmpty();
    assertThat(CheckCompatibility.writeCompatibilityErrors(version, ordered, reversed, false))
        .isEmpty();
    assertThat(CheckCompatibility.typeCompatibilityErrors(version, ordered, reversed)).isNotEmpty();
    assertThat(CheckCompatibility.typeCompatibilityErrors(version, ordered, reversed, false))
        .isEmpty();
    assertThat(CheckCompatibility.readCompatibilityErrors(version, ordered, reversed)).isEmpty();

    for (Type.PrimitiveType zoned :
        List.of(Types.TimestampType.withZone(), Types.TimestampNanoType.withZone())) {
      Schema dates = schema(Types.DateType.get(), false);
      assertThat(CheckCompatibility.readCompatibilityErrors(version, schema(zoned, false), dates))
          .isNotEmpty();
      assertThat(CheckCompatibility.writeCompatibilityErrors(version, schema(zoned, false), dates))
          .isNotEmpty();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {Integer.MIN_VALUE, -1, 0})
  void rejectsUnknownVersionEvenForEmptySchemas(int version) {
    Schema empty = new Schema();
    List<Runnable> checks =
        List.of(
            () -> CheckCompatibility.readCompatibilityErrors(version, empty, empty),
            () -> CheckCompatibility.writeCompatibilityErrors(version, empty, empty),
            () -> CheckCompatibility.writeCompatibilityErrors(version, empty, empty, false),
            () -> CheckCompatibility.typeCompatibilityErrors(version, empty, empty),
            () -> CheckCompatibility.typeCompatibilityErrors(version, empty, empty, false),
            () -> TypeUtil.validateWriteSchema(version, empty, empty, true, true),
            () -> TypeUtil.validateWriteSchema(version, empty, empty, false, false),
            () -> TypeUtil.validateSchema(version, "row", empty, empty, true, true),
            () -> TypeUtil.validateSchema(version, "row", empty, empty, false, false));
    for (Runnable check : checks) {
      assertThatThrownBy(check::run)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Invalid format version: %s", version);
    }
  }

  private static Schema schema(Type type, boolean nested) {
    if (nested) {
      return new Schema(
          Types.NestedField.optional(
              1,
              "s",
              Types.StructType.of(
                  Types.NestedField.optional(2, "d", type),
                  Types.NestedField.optional(3, "dates", Types.ListType.ofOptional(4, type)),
                  Types.NestedField.optional(
                      5,
                      "by_name",
                      Types.MapType.ofOptional(6, 7, Types.StringType.get(), type)))));
    }
    return new Schema(Types.NestedField.optional(1, "d", type));
  }
}
