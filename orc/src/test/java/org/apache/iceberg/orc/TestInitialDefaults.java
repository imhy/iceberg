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

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.iceberg.expressions.Literal;
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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestInitialDefaults {
  @TempDir private File temp;
  private int fileId;
  private static final Types.NestedField ID =
      Types.NestedField.required(1, "id", Types.IntegerType.get());

  static Stream<Type.PrimitiveType> targets() {
    return Stream.of(
        Types.DateType.get(),
        Types.TimestampType.withoutZone(),
        Types.TimestampNanoType.withoutZone());
  }

  static Stream<Arguments> modes() {
    return targets()
        .flatMap(target -> Stream.of(false, true).map(required -> Arguments.of(target, required)));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void materializesMissingFields(Type.PrimitiveType type, boolean required) throws IOException {
    Schema physical = new Schema(ID);
    Types.NestedField field =
        Types.NestedField.optional("d")
            .withId(2)
            .ofType(type)
            .withInitialDefault(defaultValue(type))
            .withWriteDefault(Literal.of(text(type, 2)).to(type))
            .build();
    Schema expected = new Schema(required ? field.asRequired() : field, ID);
    InputFile file = write(physical, List.of(GenericRecord.create(physical).copy("id", 7)));
    List<Record> rows = read(file, expected, Expressions.equal("d", text(type, 1)));
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getField("d")).isEqualTo(value(type, 1));
    assertThat(rows.get(0).getField("id")).isEqualTo(7);
  }

  @ParameterizedTest
  @MethodSource("targets")
  void preservesStoredNullsAndValues(Type.PrimitiveType type) throws IOException {
    Schema schema =
        new Schema(
            ID,
            Types.NestedField.optional("d")
                .withId(2)
                .ofType(type)
                .withInitialDefault(defaultValue(type))
                .build());
    InputFile file =
        write(
            schema,
            List.of(
                GenericRecord.create(schema).copy("id", 1),
                GenericRecord.create(schema).copy("id", 2, "d", value(type, 2))));
    assertThat(read(file, schema, Expressions.alwaysTrue()))
        .extracting(r -> r.getField("d"))
        .containsExactly(null, value(type, 2));
  }

  @ParameterizedTest
  @MethodSource("targets")
  void keepsMissingAndNullParentStructsNull(Type.PrimitiveType type) throws IOException {
    Types.StructType original =
        Types.StructType.of(Types.NestedField.required(2, "id", Types.IntegerType.get()));
    Schema physical = new Schema(Types.NestedField.optional(1, "s", original));
    Types.StructType evolved =
        Types.StructType.of(
            original.fields().get(0),
            Types.NestedField.required("d")
                .withId(3)
                .ofType(type)
                .withInitialDefault(defaultValue(type))
                .build());
    Schema expected =
        new Schema(
            Types.NestedField.optional(1, "s", evolved),
            Types.NestedField.optional(
                4,
                "missing",
                Types.StructType.of(
                    Types.NestedField.required("d")
                        .withId(5)
                        .ofType(type)
                        .withInitialDefault(defaultValue(type))
                        .build())));
    InputFile file =
        write(
            physical,
            List.of(
                GenericRecord.create(physical)
                    .copy("s", GenericRecord.create(original).copy("id", 7)),
                GenericRecord.create(physical)));
    List<Record> rows = read(file, expected, Expressions.alwaysTrue());
    assertThat(((Record) rows.get(0).getField("s")).getField("d")).isEqualTo(value(type, 1));
    assertThat(rows.get(1).getField("s")).isNull();
    assertThat(rows).extracting(r -> r.getField("missing")).containsExactly(null, null);
  }

  @ParameterizedTest
  @MethodSource("targets")
  void defaultPredicatesCannotPruneMatchingRows(Type.PrimitiveType type) {
    Schema expected =
        new Schema(
            ID,
            Types.NestedField.optional("d")
                .withId(2)
                .ofType(type)
                .withInitialDefault(defaultValue(type))
                .build());
    TypeDescription projection =
        ORCSchemaUtil.buildOrcProjection(expected, ORCSchemaUtil.convert(new Schema(ID)));
    for (Expression predicate :
        List.of(
            Expressions.equal("d", text(type, 1)),
            Expressions.notEqual("d", text(type, 1)),
            Expressions.lessThan("d", text(type, 2)),
            Expressions.in("d", text(type, 1), text(type, 2)),
            Expressions.isNull("d"),
            Expressions.notNull("d"))) {
      assertThat(sarg(expected, projection, predicate).evaluate(new TruthValue[0]))
          .isEqualTo(TruthValue.YES_NO_NULL);
      assertThat(sarg(expected, projection, Expressions.not(predicate)).evaluate(new TruthValue[0]))
          .isEqualTo(TruthValue.YES_NO_NULL);
      SearchArgument and =
          sarg(expected, projection, Expressions.and(predicate, Expressions.equal("id", 7)));
      assertThat(and.getLeaves()).hasSize(1);
      assertThat(and.getLeaves().get(0).getColumnName()).isEqualTo("`id`");
      assertThat(and.evaluate(new TruthValue[] {TruthValue.NO}).isNeeded()).isFalse();
      assertThat(and.evaluate(new TruthValue[] {TruthValue.YES}).isNeeded()).isTrue();
      SearchArgument or =
          sarg(expected, projection, Expressions.or(predicate, Expressions.equal("id", 7)));
      assertThat(or.evaluate(new TruthValue[] {TruthValue.NO}).isNeeded()).isTrue();
    }
    // A physically present column with the same declared default keeps normal ORC pruning.
    TypeDescription present =
        ORCSchemaUtil.buildOrcProjection(expected, ORCSchemaUtil.convert(expected));
    assertThat(sarg(expected, present, Expressions.equal("d", text(type, 1))).getLeaves())
        .hasSize(1);
  }

  private static SearchArgument sarg(Schema schema, TypeDescription projection, Expression filter) {
    return ExpressionToSearchArgument.convert(
        Binder.bind(schema.asStruct(), filter, true), projection);
  }

  private static Literal<?> defaultValue(Type.PrimitiveType type) {
    return Literal.of(text(type, 1)).to(type);
  }

  private static String text(Type type, int days) {
    return value(type, days).toString();
  }

  private static Object value(Type type, int days) {
    LocalDate date = LocalDate.ofEpochDay(days);
    return type == Types.DateType.get() ? date : date.atStartOfDay();
  }

  private InputFile write(Schema schema, List<Record> records) throws IOException {
    File file = new File(temp, "defaults-" + fileId++ + ".orc");
    try (FileAppender<Record> writer =
        ORC.write(Files.localOutput(file))
            .schema(schema)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .build()) {
      writer.addAll(records);
    }
    return Files.localInput(file);
  }

  private static List<Record> read(InputFile file, Schema schema, Expression filter)
      throws IOException {
    List<Record> records = new ArrayList<>();
    try (CloseableIterable<Record> reader =
        ORC.read(file)
            .project(schema)
            .filter(filter)
            .createReaderFunc(s -> GenericOrcReader.buildReader(schema, s))
            .build()) {
      for (Record row : reader) {
        records.add(row.copy());
      }
    }
    return records;
  }
}
