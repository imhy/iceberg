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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestDateToTimestampSchemaUpdate {
  private static final Schema DATE_SCHEMA =
      new Schema(Types.NestedField.optional(1, "d", Types.DateType.get()));
  @TempDir private File temp;

  static Stream<Type.PrimitiveType> targets() {
    return Stream.of(Types.TimestampType.withoutZone(), Types.TimestampNanoType.withoutZone());
  }

  static Stream<Arguments> versionsAndTargets() {
    return Stream.of(1, 2, 3, 4)
        .flatMap(version -> targets().map(type -> Arguments.of(version, type)));
  }

  @AfterEach
  void cleanup() {
    TestTables.clearTables();
  }

  @ParameterizedTest
  @MethodSource("versionsAndTargets")
  void publicPromotionHonorsFormatVersion(int version, Type.PrimitiveType target) {
    Table table =
        TestTables.create(temp, "test", DATE_SCHEMA, PartitionSpec.unpartitioned(), version);
    Schema original = table.schema();
    UpdateSchema update = table.updateSchema();
    if (version < 3) {
      assertThatThrownBy(() -> update.allowIncompatibleChanges().updateColumn("d", target))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Cannot change column type");
      assertThat(update.apply().asStruct()).isEqualTo(original.asStruct());
      return;
    }
    update.updateColumn("d", target);
    assertThat(update.apply().findType("d")).isEqualTo(target);
    update.commit();
    table.refresh();
    assertThat(table.schema().findField("d").fieldId())
        .isEqualTo(original.findField("d").fieldId());
    assertThat(table.schema().findField("d").isOptional()).isTrue();
    assertThat(table.schema().findType("d")).isEqualTo(target);
    assertThat(table.schema().schemaId()).isNotEqualTo(original.schemaId());
    assertThat(table.schemas().get(original.schemaId()).asStruct()).isEqualTo(original.asStruct());
    int schemaId = table.schema().schemaId();
    table.updateSchema().updateColumn("d", target).commit();
    assertThat(table.schema().schemaId()).isEqualTo(schemaId);
  }

  @ParameterizedTest
  @MethodSource("versionsAndTargets")
  void rejectsZonedTargets(int version, Type.PrimitiveType target) {
    Table table = create(DATE_SCHEMA, PartitionSpec.unpartitioned(), version);
    Type.PrimitiveType zoned =
        target.typeId() == Type.TypeID.TIMESTAMP
            ? Types.TimestampType.withZone()
            : Types.TimestampNanoType.withZone();
    UpdateSchema update = table.updateSchema().allowIncompatibleChanges();
    assertThatThrownBy(() -> update.updateColumn("d", zoned))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot change column type");
    assertThat(update.apply().asStruct()).isEqualTo(table.schema().asStruct());
  }

  @ParameterizedTest
  @MethodSource("targets")
  void preservesDefaultsAndFieldMetadata(Type.PrimitiveType target) {
    Schema schema =
        new Schema(
            List.of(
                Types.NestedField.required("d")
                    .withId(1)
                    .ofType(Types.DateType.get())
                    .withDoc("date column")
                    .withInitialDefault(date(-1))
                    .withWriteDefault(date(1))
                    .build(),
                Types.NestedField.optional(2, "empty", Types.DateType.get())),
            Set.of(1));
    List<MetadataUpdate> captured = Lists.newArrayList();
    TestTables.TestTableOperations ops =
        new TestTables.TestTableOperations("test", temp) {
          @Override
          public void commit(TableMetadata base, TableMetadata updated) {
            super.commit(base, updated);
            captured.clear();
            captured.addAll(updated.changes());
          }
        };
    Table table =
        TestTables.create(
            temp, "test", schema, PartitionSpec.unpartitioned(), SortOrder.unsorted(), 3, ops);
    TableMetadata original = metadata(table);
    table.updateSchema().updateColumn("d", target).updateColumn("empty", target).commit();
    List<MetadataUpdate> changes = List.copyOf(captured);
    assertThat(changes).anyMatch(change -> change instanceof MetadataUpdate.AddSchema);
    assertThat(changes).anyMatch(change -> change instanceof MetadataUpdate.SetCurrentSchema);
    TableMetadata result = reload(table);
    Types.NestedField field = result.schema().findField("d");
    assertThat(field.fieldId()).isEqualTo(original.schema().findField("d").fieldId());
    assertThat(field.isRequired()).isTrue();
    assertThat(field.doc()).isEqualTo("date column");
    assertThat(field.initialDefault()).isEqualTo(-unitsPerDay(target));
    assertThat(field.writeDefault()).isEqualTo(unitsPerDay(target));
    assertThat(result.schema().identifierFieldIds())
        .isEqualTo(original.schema().identifierFieldIds());
    assertThat(result.schema().findField("empty").initialDefault()).isNull();
    assertThat(result.schema().findField("empty").writeDefault()).isNull();
    assertThat(result.schemasById().get(original.currentSchemaId()).findField("d").initialDefault())
        .isEqualTo(-1);
    assertThat(result.lastColumnId()).isEqualTo(original.lastColumnId());
    TableMetadata.Builder replay = TableMetadata.buildFrom(original);
    for (MetadataUpdate change : changes) {
      MetadataUpdateParser.fromJson(MetadataUpdateParser.toJson(change)).applyTo(replay);
    }
    assertThat(replay.build().schema().asStruct()).isEqualTo(result.schema().asStruct());
  }

  @ParameterizedTest
  @MethodSource("targets")
  void convertsBoundaryDefaultsAndRejectsOverflow(Type.PrimitiveType target) {
    long units = unitsPerDay(target);
    int minimum = (int) (Long.MIN_VALUE / units);
    int maximum = (int) (Long.MAX_VALUE / units);
    int index = 0;
    for (int day : new int[] {minimum, -1, 0, 1, maximum, minimum - 1, maximum + 1}) {
      for (boolean initial : new boolean[] {true, false}) {
        Types.NestedField.Builder field =
            Types.NestedField.optional("d").withId(1).ofType(Types.DateType.get());
        if (initial) {
          field.withInitialDefault(date(day));
        } else {
          field.withWriteDefault(date(day));
        }
        Table table =
            TestTables.create(
                new File(temp, "defaults-" + index),
                "defaults-" + index++,
                new Schema(field.build()),
                PartitionSpec.unpartitioned(),
                3);
        UpdateSchema update = table.updateSchema();
        if (day < minimum || day > maximum) {
          assertThatThrownBy(() -> update.updateColumn("d", target))
              .isInstanceOf(ArithmeticException.class)
              .hasMessage("long overflow");
          assertThat(update.apply().asStruct()).isEqualTo(table.schema().asStruct());
        } else {
          update.updateColumn("d", target).commit();
          Types.NestedField converted = reload(table).schema().findField("d");
          assertThat(initial ? converted.initialDefault() : converted.writeDefault())
              .isEqualTo(day * units);
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("targets")
  void composesAddedFieldsRenamesAndDefaults(Type.PrimitiveType target) {
    Table table = create(DATE_SCHEMA, PartitionSpec.unpartitioned(), 3);
    UpdateSchema update = table.updateSchema();
    update
        .updateColumnDefault("d", date(-1))
        .renameColumn("d", "renamed")
        .updateColumn("d", target)
        .updateColumnDoc("d", "promoted")
        .addColumn("added", Types.DateType.get(), "added date", date(1))
        .updateColumn("added", target);
    update.commit();
    Schema result = reload(table).schema();
    assertThat(result.findField("renamed").writeDefault()).isEqualTo(-unitsPerDay(target));
    assertThat(result.findField("renamed").doc()).isEqualTo("promoted");
    assertThat(result.findField("added").initialDefault()).isEqualTo(unitsPerDay(target));
    assertThat(result.findField("added").writeDefault()).isEqualTo(unitsPerDay(target));
    table.updateSchema().updateColumnDefault("renamed", Literal.of(123L).to(target)).commit();
    long expected = target.typeId() == Type.TypeID.TIMESTAMP ? 123L : 123_000L;
    assertThat(table.schema().findField("renamed").writeDefault()).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("targets")
  void nestedFieldsAndMapKeys(Type.PrimitiveType target) {
    Type date = Types.DateType.get();
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1, "struct", Types.StructType.of(Types.NestedField.required(2, "d", date))),
            Types.NestedField.optional(3, "list", Types.ListType.ofOptional(4, date)),
            Types.NestedField.optional(
                5,
                "list_struct",
                Types.ListType.ofRequired(
                    6, Types.StructType.of(Types.NestedField.optional(7, "d", date)))),
            Types.NestedField.optional(
                8, "map", Types.MapType.ofOptional(9, 10, Types.StringType.get(), date)),
            Types.NestedField.optional(
                11,
                "map_struct",
                Types.MapType.ofRequired(
                    12,
                    13,
                    Types.StringType.get(),
                    Types.StructType.of(Types.NestedField.required(14, "d", date)))),
            Types.NestedField.optional(
                15, "date_key", Types.MapType.ofOptional(16, 17, date, Types.StringType.get())));
    Table table = create(schema, PartitionSpec.unpartitioned(), 3);
    Schema original = table.schema();
    List<String> names =
        List.of(
            "struct.d", "list.element", "list_struct.element.d", "map.value", "map_struct.value.d");
    UpdateSchema update = table.updateSchema();
    for (String name : names) {
      update.updateColumn(name, target);
    }
    update.commit();
    Schema evolved = reload(table).schema();
    for (String name : names) {
      assertThat(evolved.findType(name)).isEqualTo(target);
      assertThat(evolved.findField(name).fieldId()).isEqualTo(original.findField(name).fieldId());
      assertThat(evolved.findField(name).isOptional())
          .isEqualTo(original.findField(name).isOptional());
    }
    assertThatThrownBy(() -> table.updateSchema().updateColumn("date_key.key", target).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot update map keys");
    assertThat(table.schema().asStruct()).isEqualTo(evolved.asStruct());
  }

  static Stream<Arguments> partitionTargets() {
    return targets()
        .flatMap(
            target ->
                Stream.of("year", "month", "day", "void")
                    .map(transform -> Arguments.of(target, transform)));
  }

  @ParameterizedTest
  @MethodSource("partitionTargets")
  void preservesPartitionValuesAndRebindsLoadedTransforms(
      Type.PrimitiveType target, String transform) {
    PartitionSpec spec =
        PartitionSpec.builderFor(DATE_SCHEMA)
            .add(1, 1000, "part", Transforms.fromString(Types.DateType.get(), transform))
            .build();
    Table table = create(DATE_SCHEMA, spec, 3);
    reload(table);
    PartitionSpec original = table.spec();
    table.updateSchema().updateColumn("d", target).commit();
    PartitionSpec evolved = reload(table).spec();
    assertThat(evolved.specId()).isEqualTo(original.specId());
    assertThat(evolved.fields().get(0).fieldId()).isEqualTo(original.fields().get(0).fieldId());
    assertThat(evolved.fields().get(0).sourceId()).isEqualTo(original.fields().get(0).sourceId());
    assertThat(evolved.fields().get(0).name()).isEqualTo("part");
    if (transform.equals("void")) {
      assertThat(evolved.partitionType().fields().get(0).type()).isEqualTo(target);
    } else {
      assertThat(evolved.partitionType()).isEqualTo(original.partitionType());
    }
    for (String value :
        List.of(
            "1968-02-29", "1969-12-31", "1970-01-01", "1970-01-02", "2000-02-29", "2024-12-31")) {
      int day = (int) LocalDate.parse(value).toEpochDay();
      assertThat(
              partitionValue(
                  evolved.fields().get(0).transform(), target, day * unitsPerDay(target)))
          .isEqualTo(
              partitionValue(original.fields().get(0).transform(), Types.DateType.get(), day));
    }
  }

  @ParameterizedTest
  @MethodSource("targets")
  void rejectsIncompatibleCurrentAndHistoricalPartitions(Type.PrimitiveType target) {
    int index = 0;
    for (String transform : List.of("identity", "bucket[16]", "future-transform")) {
      for (boolean historical :
          transform.equals("future-transform")
              ? new boolean[] {false}
              : new boolean[] {false, true}) {
        PartitionSpec spec =
            PartitionSpec.builderFor(DATE_SCHEMA)
                .add(1, 1000, "part", Transforms.fromString(transform))
                .build();
        Table table =
            TestTables.create(
                new File(temp, "partition-" + index), "partition-" + index++, DATE_SCHEMA, spec, 3);
        if (historical) {
          table.updateSpec().removeField("part").commit();
          assertThat(table.spec().isUnpartitioned()).isTrue();
        }
        Schema original = table.schema();
        UpdateSchema update =
            table.updateSchema().allowIncompatibleChanges().renameColumn("d", "renamed");
        assertThatThrownBy(() -> update.updateColumn("d", target))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("incompatible transform");
        assertThat(update.apply().findType("renamed")).isEqualTo(Types.DateType.get());
        assertThat(table.schema().asStruct()).isEqualTo(original.asStruct());
        assertThatThrownBy(
                () ->
                    table
                        .updateSchema()
                        .unionByNameWith(new Schema(Types.NestedField.optional(1, "d", target))))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("incompatible transform");
      }
    }
    assertThat(Transforms.<Integer>bucket(16).bind(Types.DateType.get()).apply(1))
        .isNotEqualTo(Transforms.<Long>bucket(16).bind(target).apply(unitsPerDay(target)));
  }

  @ParameterizedTest
  @MethodSource("targets")
  void sortOrdersRetainTheirMeaning(Type.PrimitiveType target) {
    int index = 0;
    for (Term term :
        List.<Term>of(
            Expressions.ref("d"),
            Expressions.year("d"),
            Expressions.month("d"),
            Expressions.day("d"),
            Expressions.bucket("d", 16))) {
      SortOrder order = SortOrder.builderFor(DATE_SCHEMA).desc(term, NullOrder.NULLS_FIRST).build();
      Table table =
          TestTables.create(
              new File(temp, "sort-" + index),
              "sort-" + index++,
              DATE_SCHEMA,
              PartitionSpec.unpartitioned(),
              order,
              3);
      reload(table);
      if (order.fields().get(0).transform().toString().startsWith("bucket")) {
        assertThatThrownBy(() -> table.updateSchema().updateColumn("d", target))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("sort order");
        table.replaceSortOrder().asc("d").commit();
        assertThatThrownBy(() -> table.updateSchema().updateColumn("d", target))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("sort order");
      } else {
        table.updateSchema().updateColumn("d", target).commit();
        SortOrder evolved = reload(table).sortOrder();
        assertThat(evolved.orderId()).isEqualTo(order.orderId());
        SortField field = evolved.fields().get(0);
        assertThat(field.direction()).isEqualTo(SortDirection.DESC);
        assertThat(field.nullOrder()).isEqualTo(NullOrder.NULLS_FIRST);
        assertThat(field.transform().canTransform(target)).isTrue();
        assertThat(field.transform().toString())
            .isEqualTo(order.fields().get(0).transform().toString());
        assertThat(field.sourceId()).isEqualTo(order.fields().get(0).sourceId());
        Object before = partitionValue(field.transform(), target, -unitsPerDay(target));
        Object after = partitionValue(field.transform(), target, unitsPerDay(target));
        assertThat(((Number) before).longValue()).isLessThanOrEqualTo(((Number) after).longValue());
        if (field.transform().isIdentity()) {
          assertThat(before).isEqualTo(-unitsPerDay(target));
          assertThat(after).isEqualTo(unitsPerDay(target));
        } else {
          assertThat(before)
              .isEqualTo(
                  partitionValue(order.fields().get(0).transform(), Types.DateType.get(), -1));
          assertThat(after)
              .isEqualTo(
                  partitionValue(order.fields().get(0).transform(), Types.DateType.get(), 1));
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("targets")
  void publicUnionAndTransaction(Type.PrimitiveType target) {
    Table table = create(DATE_SCHEMA, PartitionSpec.unpartitioned(), 2);
    Schema desired = new Schema(Types.NestedField.optional(1, "d", target));
    assertThatThrownBy(() -> table.updateSchema().unionByNameWith(desired))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot change column type");
    Transaction tx = table.newTransaction();
    tx.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();
    tx.updateSchema().unionByNameWith(desired).commit();
    assertThat(table.schema().findType("d")).isEqualTo(Types.DateType.get());
    tx.commitTransaction();
    assertThat(reload(table).schema().findType("d")).isEqualTo(target);
    assertThatThrownBy(() -> table.updateSchema().updateColumn("d", Types.DateType.get()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot change column type");
  }

  @ParameterizedTest
  @MethodSource("targets")
  void concurrentPartitionChangeCannotBypassValidation(Type.PrimitiveType target) {
    Table table = create(DATE_SCHEMA, PartitionSpec.unpartitioned(), 3);
    UpdateSchema stale = table.updateSchema().updateColumn("d", target);
    table.updateSpec().addField(Expressions.bucket("d", 16)).commit();
    assertThatThrownBy(stale::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Cannot commit changes based on stale metadata");
    assertThat(table.schema().findType("d")).isEqualTo(Types.DateType.get());
    assertThatThrownBy(() -> table.updateSchema().updateColumn("d", target))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("incompatible transform");
  }

  @ParameterizedTest
  @MethodSource("targets")
  void unionConvertsExistingDefaults(Type.PrimitiveType target) {
    Schema original =
        new Schema(
            Types.NestedField.optional("d")
                .withId(1)
                .ofType(Types.DateType.get())
                .withInitialDefault(date(-1))
                .withWriteDefault(date(0))
                .build());
    Table table = create(original, PartitionSpec.unpartitioned(), 3);
    Schema desired =
        new Schema(
            Types.NestedField.optional("d")
                .withId(1)
                .ofType(target)
                .withWriteDefault(Literal.of(86_400_000_000L).to(target))
                .build());
    table.updateSchema().unionByNameWith(desired).commit();
    Types.NestedField result = reload(table).schema().findField("d");
    assertThat(result.initialDefault()).isEqualTo(-unitsPerDay(target));
    assertThat(result.writeDefault()).isEqualTo(unitsPerDay(target));
  }

  static Stream<Arguments> unionVersionsAndTargets() {
    return Stream.concat(
        Stream.of(1, 2).map(v -> Arguments.of(v, Types.TimestampType.withoutZone())),
        Stream.of(3, 4).flatMap(v -> targets().map(t -> Arguments.of(v, t))));
  }

  @ParameterizedTest
  @MethodSource("unionVersionsAndTargets")
  void reverseUnionHonorsFormatVersion(int version, Type.PrimitiveType target) {
    Schema original = new Schema(Types.NestedField.optional(1, "d", target));
    Table table = create(original, PartitionSpec.unpartitioned(), version);
    if (version < 3) {
      assertThatThrownBy(() -> table.updateSchema().unionByNameWith(DATE_SCHEMA))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Cannot change column type");
    } else {
      table.updateSchema().unionByNameWith(DATE_SCHEMA).commit();
      assertThat(reload(table).schema().asStruct()).isEqualTo(original.asStruct());
    }
    assertThatThrownBy(() -> table.updateSchema().updateColumn("d", Types.DateType.get()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot change column type");
  }

  @ParameterizedTest
  @MethodSource("targets")
  void reverseUnionConvertsIncomingDefaults(Type.PrimitiveType target) {
    Schema original =
        new Schema(
            Types.NestedField.optional("d")
                .withId(1)
                .ofType(target)
                .withInitialDefault(Literal.of(-86_400_000_000L).to(target))
                .withWriteDefault(Literal.of(0L).to(target))
                .build());
    Table table = create(original, PartitionSpec.unpartitioned(), 3);
    Schema incoming =
        new Schema(
            Types.NestedField.optional("D")
                .withId(99)
                .ofType(Types.DateType.get())
                .withInitialDefault(date(10))
                .withWriteDefault(date(1))
                .build());
    table.updateSchema().caseSensitive(false).unionByNameWith(incoming).commit();
    Types.NestedField result = reload(table).schema().findField("d");
    assertThat(result.type()).isEqualTo(target);
    assertThat(result.fieldId()).isEqualTo(1);
    assertThat(result.initialDefault()).isEqualTo(-unitsPerDay(target));
    assertThat(result.writeDefault()).isEqualTo(unitsPerDay(target));
    // Repeating the union preserves the converted value.
    table.updateSchema().caseSensitive(false).unionByNameWith(incoming).commit();
    assertThat(reload(table).schema().findField("d")).isEqualTo(result);
  }

  @ParameterizedTest
  @MethodSource("targets")
  void reverseUnionChecksIncomingDefaultRange(Type.PrimitiveType target) {
    long units = unitsPerDay(target);
    for (int day :
        new int[] {
          (int) (Long.MIN_VALUE / units),
          (int) (Long.MAX_VALUE / units),
          (int) (Long.MIN_VALUE / units) - 1,
          (int) (Long.MAX_VALUE / units) + 1
        }) {
      Schema original = new Schema(Types.NestedField.optional(1, "d", target));
      Table table =
          TestTables.create(
              new File(temp, "union-" + day),
              "union-" + day,
              original,
              PartitionSpec.unpartitioned(),
              3);
      Schema incoming =
          new Schema(
              Types.NestedField.optional("d")
                  .withId(1)
                  .ofType(Types.DateType.get())
                  .withWriteDefault(date(day))
                  .build());
      UpdateSchema update = table.updateSchema();
      if (day < Long.MIN_VALUE / units || day > Long.MAX_VALUE / units) {
        assertThatThrownBy(() -> update.unionByNameWith(incoming))
            .isInstanceOf(ArithmeticException.class)
            .hasMessage("long overflow");
        assertThat(reload(table).schema().asStruct()).isEqualTo(original.asStruct());
      } else {
        update.unionByNameWith(incoming).commit();
        assertThat(reload(table).schema().findField("d").writeDefault()).isEqualTo(day * units);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("targets")
  void unionsNestedDatesInBothDirections(Type.PrimitiveType target) {
    Schema original = unionNestedSchema(Types.DateType.get(), date(0));
    Table table = create(original, PartitionSpec.unpartitioned(), 3);
    table
        .updateSchema()
        .unionByNameWith(unionNestedSchema(target, Literal.of(2 * 86_400_000_000L).to(target)))
        .commit();
    table.updateSchema().unionByNameWith(unionNestedSchema(Types.DateType.get(), date(1))).commit();
    Schema result = reload(table).schema();
    assertThat(result.findType("s.d")).isEqualTo(target);
    assertThat(result.findType("s.dates.element")).isEqualTo(target);
    assertThat(result.findType("s.by_name.value")).isEqualTo(target);
    assertThat(result.findType("s.by_name.key")).isEqualTo(Types.StringType.get());
    assertThat(result.findField("s.d").initialDefault()).isEqualTo(-unitsPerDay(target));
    assertThat(result.findField("s.d").writeDefault()).isEqualTo(unitsPerDay(target));
  }

  @ParameterizedTest
  @MethodSource("targets")
  void reverseUnionRejectsZonedTargets(Type.PrimitiveType target) {
    Type.PrimitiveType zoned =
        target instanceof Types.TimestampNanoType
            ? Types.TimestampNanoType.withZone()
            : Types.TimestampType.withZone();
    Schema original = new Schema(Types.NestedField.optional(1, "d", zoned));
    Table table = create(original, PartitionSpec.unpartitioned(), 3);
    assertThatThrownBy(() -> table.updateSchema().unionByNameWith(DATE_SCHEMA))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot change column type");
    assertThat(reload(table).schema().asStruct()).isEqualTo(original.asStruct());
  }

  private static Schema unionNestedSchema(Type.PrimitiveType type, Literal<?> writeDefault) {
    Literal<?> initial =
        type.equals(Types.DateType.get()) ? date(-1) : Literal.of(-86_400_000_000L).to(type);
    return new Schema(
        Types.NestedField.optional(
            1,
            "s",
            Types.StructType.of(
                Types.NestedField.optional("d")
                    .withId(2)
                    .ofType(type)
                    .withInitialDefault(initial)
                    .withWriteDefault(writeDefault)
                    .build(),
                Types.NestedField.optional(3, "dates", Types.ListType.ofOptional(4, type)),
                Types.NestedField.optional(
                    5, "by_name", Types.MapType.ofOptional(6, 7, Types.StringType.get(), type)))));
  }

  @ParameterizedTest
  @MethodSource("targets")
  void reverseUnionUsesTransactionVersion(Type.PrimitiveType target) {
    Table table = create(DATE_SCHEMA, PartitionSpec.unpartitioned(), 2);
    Transaction tx = table.newTransaction();
    tx.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();
    tx.updateSchema().updateColumn("d", target).commit();
    Schema incoming =
        new Schema(
            Types.NestedField.optional("d")
                .withId(1)
                .ofType(Types.DateType.get())
                .withWriteDefault(date(1))
                .build());
    tx.updateSchema().unionByNameWith(incoming).commit();
    assertThat(table.schema().findType("d")).isEqualTo(Types.DateType.get());
    tx.commitTransaction();
    assertThat(reload(table).schema().findType("d")).isEqualTo(target);
    assertThat(table.schema().findField("d").writeDefault()).isEqualTo(unitsPerDay(target));
  }

  @ParameterizedTest
  @MethodSource("targets")
  void transactionCannotReplayAcrossIncompatiblePartitionChange(Type.PrimitiveType target) {
    Table table = create(DATE_SCHEMA, PartitionSpec.unpartitioned(), 3);
    Transaction tx = table.newTransaction();
    tx.updateSchema().updateColumn("d", target).commit();
    table.updateSpec().addField(Expressions.bucket("d", 16)).commit();
    assertThatThrownBy(tx::commitTransaction)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Table metadata refresh is required");
    table.refresh();
    assertThat(table.schema().findType("d")).isEqualTo(Types.DateType.get());
    assertThat(table.spec().fields()).hasSize(1);
  }

  @ParameterizedTest
  @MethodSource("targets")
  void preservesRetainedTemporalSpecsAndDeletedSources(Type.PrimitiveType target) {
    PartitionSpec dateSpec = PartitionSpec.builderFor(DATE_SCHEMA).day("d").build();
    Table table = create(DATE_SCHEMA, dateSpec, 3);
    int specId = table.spec().specId();
    table.updateSpec().removeField("d_day").commit();
    reload(table);
    table.updateSchema().updateColumn("d", target).commit();
    TableMetadata result = reload(table);
    assertThat(result.spec()).matches(PartitionSpec::isUnpartitioned);
    assertThat(result.specsById().get(specId).fields().get(0).transform().canTransform(target))
        .isTrue();
    assertThat(
            partitionValue(
                result.specsById().get(specId).fields().get(0).transform(),
                target,
                unitsPerDay(target)))
        .isEqualTo(1);

    Schema schema =
        new Schema(
            DATE_SCHEMA.findField("d"),
            Types.NestedField.optional(2, "retired", Types.DateType.get()));
    PartitionSpec retiredSpec = PartitionSpec.builderFor(schema).day("retired").build();
    SortOrder retiredOrder = SortOrder.builderFor(schema).asc(Expressions.year("retired")).build();
    Table retired =
        TestTables.create(
            new File(temp, "retired"), "retired", schema, retiredSpec, retiredOrder, 3);
    int retiredId = retired.schema().findField("retired").fieldId();
    int retiredSpecId = retired.spec().specId();
    int retiredOrderId = retired.sortOrder().orderId();
    retired.updateSpec().removeField("retired_day").commit();
    retired.replaceSortOrder().asc("d").commit();
    retired.updateSchema().deleteColumn("retired").commit();
    reload(retired);
    retired.updateSchema().updateColumn("d", target).commit();
    TableMetadata after = reload(retired);
    assertThat(after.schema().findField("retired")).isNull();
    assertThat(after.schema().findType("d")).isEqualTo(target);
    assertThat(after.specsById().get(retiredSpecId).fields().get(0).sourceId())
        .isEqualTo(retiredId);
    assertThat(after.sortOrdersById().get(retiredOrderId).fields().get(0).sourceId())
        .isEqualTo(retiredId);
  }

  private Table create(Schema schema, PartitionSpec spec, int version) {
    return TestTables.create(temp, "test", schema, spec, version);
  }

  private static TableMetadata metadata(Table table) {
    return ((BaseTable) table).operations().current();
  }

  private static TableMetadata reload(Table table) {
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata before = ops.current();
    TableMetadata parsed = TableMetadataParser.fromJson(TableMetadataParser.toJson(before));
    ops.commit(before, parsed);
    table.refresh();
    return ops.current();
  }

  private static Literal<?> date(int day) {
    return Literal.of(day).to(Types.DateType.get());
  }

  private static long unitsPerDay(Type type) {
    return type.typeId() == Type.TypeID.TIMESTAMP ? 86_400_000_000L : 86_400_000_000_000L;
  }

  @ParameterizedTest
  @MethodSource("targets")
  @SuppressWarnings("deprecation")
  void rebindsTypedIdentitySortTransform(Type.PrimitiveType target) {
    SortOrder order =
        SortOrder.builderFor(DATE_SCHEMA)
            .addSortField(
                Transforms.identity(Types.DateType.get()),
                1,
                SortDirection.ASC,
                NullOrder.NULLS_FIRST)
            .build();
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            DATE_SCHEMA,
            PartitionSpec.unpartitioned(),
            order,
            temp.toString(),
            Map.of("format-version", "3"));
    Schema promoted = new Schema(Types.NestedField.optional(1, "d", target));
    TableMetadata updated = metadata.updateSchema(promoted);
    Transform<?, ?> transform = updated.sortOrder().fields().get(0).transform();
    assertThat(transform.canTransform(target)).isTrue();
    assertThat(transform).isSameAs(Transforms.identity());
    assertThat(partitionValue(transform, target, unitsPerDay(target)))
        .isEqualTo(unitsPerDay(target));
  }

  @SuppressWarnings("unchecked")
  private static Object partitionValue(Transform<?, ?> transform, Type type, Object value) {
    return ((Transform<Object, Object>) transform).bind(type).apply(value);
  }
}
