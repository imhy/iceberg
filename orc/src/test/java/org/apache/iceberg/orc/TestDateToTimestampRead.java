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
package org.apache.iceberg.orc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.io.sarg.SearchArgument;
import org.apache.orc.storage.ql.io.sarg.SearchArgument.TruthValue;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestDateToTimestampRead {
  @TempDir private File temp;
  private int fileId;
  private static final Schema DATE = schema(Types.DateType.get());

  static Stream<Type.PrimitiveType> targets() {
    return Stream.of(Types.TimestampType.withoutZone(), Types.TimestampNanoType.withoutZone());
  }

  @ParameterizedTest
  @MethodSource("targets")
  void readsPhysicalDates(Type.PrimitiveType target) throws IOException {
    long units = unitsPerDay(target);
    Integer[] days = {
      (int) (Long.MIN_VALUE / units), -1, 0, 1, (int) (Long.MAX_VALUE / units), null
    };
    InputFile file = writeDates(days);
    List<Object> expected = new ArrayList<>();
    for (Integer day : days) {
      expected.add(day == null ? null : LocalDate.ofEpochDay(day).atStartOfDay());
    }
    assertThat(read(file, schema(target)))
        .extracting(r -> r.get(0))
        .containsExactlyElementsOf(expected);
    assertThat(read(file, DATE))
        .extracting(r -> r.get(0))
        .containsExactlyElementsOf(
            Arrays.stream(days).map(d -> d == null ? null : LocalDate.ofEpochDay(d)).toList());
  }

  @ParameterizedTest
  @MethodSource("targets")
  void checksOverflow(Type.PrimitiveType target) throws IOException {
    long units = unitsPerDay(target);
    for (int day :
        new int[] {(int) (Long.MIN_VALUE / units) - 1, (int) (Long.MAX_VALUE / units) + 1}) {
      InputFile file = writeDates(day);
      assertThatThrownBy(() -> read(file, schema(target))).isInstanceOf(ArithmeticException.class);
    }
  }

  @ParameterizedTest
  @MethodSource("targets")
  void preservesPhysicalProjection(Type.PrimitiveType target) {
    TypeDescription physical = ORCSchemaUtil.convert(DATE);
    TypeDescription projection = ORCSchemaUtil.buildOrcProjection(schema(target), physical);
    assertThat(projection.toString()).isEqualTo(physical.toString());
    assertThat(projection.getChildren()).isEqualTo(physical.getChildren());
    Type.PrimitiveType zoned =
        target instanceof Types.TimestampNanoType
            ? Types.TimestampNanoType.withZone()
            : Types.TimestampType.withZone();
    assertThatThrownBy(() -> ORCSchemaUtil.buildOrcProjection(schema(zoned), physical))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Can not promote DATE");
  }

  @ParameterizedTest
  @MethodSource("targets")
  void promotesNestedDates(Type.PrimitiveType target) throws IOException {
    Schema original = nestedSchema(Types.DateType.get());
    GenericRecord nested = GenericRecord.create(original.findType("s").asStructType());
    LocalDate day = LocalDate.ofEpochDay(-1);
    nested.set(0, day);
    nested.set(1, Arrays.asList(day, null));
    nested.set(2, Map.of("key", day));
    GenericRecord root = GenericRecord.create(original);
    root.set(0, nested);
    InputFile file = write(original, List.of(root));
    Record actual = (Record) read(file, nestedSchema(target)).get(0).get(0);
    assertThat(actual.get(0)).isEqualTo(day.atStartOfDay());
    assertThat(actual.get(1)).isEqualTo(Arrays.asList(day.atStartOfDay(), null));
    assertThat(actual.get(2)).isEqualTo(Map.of("key", day.atStartOfDay()));
  }

  @ParameterizedTest
  @MethodSource("targets")
  void keepsPromotedPredicatesConservative(Type.PrimitiveType target) {
    Schema expected =
        new Schema(
            Types.NestedField.optional(1, "d", target),
            Types.NestedField.optional(2, "id", Types.IntegerType.get()));
    Schema original =
        new Schema(
            Types.NestedField.optional(1, "d", Types.DateType.get()),
            Types.NestedField.optional(2, "id", Types.IntegerType.get()));
    TypeDescription physical = ORCSchemaUtil.convert(original);
    for (Expression predicate :
        List.of(
            Expressions.equal("d", 0L), Expressions.notEqual("d", 0L),
            Expressions.lessThan("d", 1L), Expressions.lessThanOrEqual("d", -1L),
            Expressions.greaterThan("d", 1L), Expressions.greaterThanOrEqual("d", -1L),
            Expressions.in("d", 0L, 1L), Expressions.notIn("d", 0L, 1L),
            Expressions.isNull("d"), Expressions.notNull("d"))) {
      assertThat(sarg(expected, physical, predicate).evaluate(new TruthValue[0]))
          .isEqualTo(TruthValue.YES_NO_NULL);
      assertThat(sarg(expected, physical, Expressions.not(predicate)).evaluate(new TruthValue[0]))
          .isEqualTo(TruthValue.YES_NO_NULL);
      SearchArgument and =
          sarg(expected, physical, Expressions.and(predicate, Expressions.equal("id", 7)));
      assertThat(and.getLeaves()).hasSize(1);
      assertThat(and.getLeaves().get(0).getColumnName()).isEqualTo("`id`");
      assertThat(and.evaluate(new TruthValue[] {TruthValue.NO}).isNeeded()).isFalse();
      assertThat(and.evaluate(new TruthValue[] {TruthValue.YES}).isNeeded()).isTrue();
      SearchArgument or =
          sarg(expected, physical, Expressions.or(predicate, Expressions.equal("id", 7)));
      assertThat(or.evaluate(new TruthValue[] {TruthValue.NO}).isNeeded()).isTrue();
    }
    // A file already written as timestamp keeps its normal timestamp pruning.
    SearchArgument current =
        sarg(expected, ORCSchemaUtil.convert(expected), Expressions.equal("d", 0L));
    assertThat(current.getLeaves()).hasSize(1);
    assertThat(current.evaluate(new TruthValue[] {TruthValue.NO}).isNeeded()).isFalse();
  }

  private static SearchArgument sarg(Schema schema, TypeDescription physical, Expression filter) {
    return ExpressionToSearchArgument.convert(
        Binder.bind(schema.asStruct(), filter, true), physical);
  }

  private InputFile writeDates(Integer... days) throws IOException {
    List<Record> records = new ArrayList<>();
    for (Integer day : days) {
      GenericRecord row = GenericRecord.create(DATE);
      row.set(0, day == null ? null : LocalDate.ofEpochDay(day));
      records.add(row);
    }
    return write(DATE, records);
  }

  private InputFile write(Schema schema, List<Record> records) throws IOException {
    File file = new File(temp, "dates-" + fileId++ + ".orc");
    try (FileAppender<Record> writer =
        ORC.write(Files.localOutput(file))
            .schema(schema)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .build()) {
      writer.addAll(records);
    }
    return Files.localInput(file);
  }

  private static List<Record> read(InputFile file, Schema schema) throws IOException {
    List<Record> result = new ArrayList<>();
    try (CloseableIterable<Record> reader =
        ORC.read(file)
            .project(schema)
            .createReaderFunc(s -> GenericOrcReader.buildReader(schema, s))
            .build()) {
      for (Record row : reader) {
        result.add(row.copy());
      }
    }
    return result;
  }

  private static Schema schema(Type type) {
    return new Schema(Types.NestedField.optional(1, "d", type));
  }

  private static Schema nestedSchema(Type type) {
    return new Schema(
        Types.NestedField.optional(
            1,
            "s",
            Types.StructType.of(
                Types.NestedField.optional(2, "d", type),
                Types.NestedField.optional(3, "dates", Types.ListType.ofOptional(4, type)),
                Types.NestedField.optional(
                    5, "by_name", Types.MapType.ofOptional(6, 7, Types.StringType.get(), type)))));
  }

  private static long unitsPerDay(Type type) {
    return type instanceof Types.TimestampNanoType ? 86_400_000_000_000L : 86_400_000_000L;
  }
}
