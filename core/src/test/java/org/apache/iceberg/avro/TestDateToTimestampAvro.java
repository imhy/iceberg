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
package org.apache.iceberg.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class TestDateToTimestampAvro {
  private static final Schema DATE = schema(Types.DateType.get());

  enum ReaderKind {
    GENERIC,
    INTERNAL,
    AVRO
  }

  static Stream<Arguments> modes() {
    return Arrays.stream(ReaderKind.values())
        .flatMap(
            reader ->
                Stream.of(
                    Arguments.of(reader, Types.TimestampType.withoutZone()),
                    Arguments.of(reader, Types.TimestampNanoType.withoutZone())));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void readsMidnightsAndBoundaries(ReaderKind reader, Type.PrimitiveType target)
      throws IOException {
    long units = unitsPerDay(target);
    Integer[] days = {
      (int) (Long.MIN_VALUE / units), -1, 0, 1, (int) (Long.MAX_VALUE / units), null
    };
    InputFile file = write(days);
    List<Object> expected = new ArrayList<>();
    for (Integer day : days) {
      expected.add(
          day == null
              ? null
              : reader == ReaderKind.GENERIC
                  ? LocalDate.ofEpochDay(day).atStartOfDay()
                  : day * units);
    }
    assertThat(read(reader, schema(target), file)).containsExactlyElementsOf(expected);
  }

  @ParameterizedTest
  @MethodSource("modes")
  void promotesNestedFields(ReaderKind reader, Type.PrimitiveType target) throws IOException {
    Schema original = nestedSchema(Types.DateType.get());
    Schema promoted = nestedSchema(target);
    GenericRecord nested = GenericRecord.create(original.findType("s").asStructType());
    LocalDate day = LocalDate.ofEpochDay(-1);
    nested.set(0, day);
    nested.set(1, Arrays.asList(day, null));
    nested.set(2, Map.of("key", day));
    GenericRecord root = GenericRecord.create(original);
    root.set(0, nested);
    InMemoryOutputFile output = new InMemoryOutputFile();
    try (FileAppender<Record> appender =
        Avro.write(output).schema(original).createWriterFunc(DataWriter::create).build()) {
      appender.add(root);
    }
    Object value = read(reader, promoted, output.toInputFile()).get(0);
    Object expected = reader == ReaderKind.GENERIC ? day.atStartOfDay() : -unitsPerDay(target);
    assertThat(field(value, 0)).isEqualTo(expected);
    assertThat(field(value, 1)).isEqualTo(Arrays.asList(expected, null));
    assertThat(((Map<?, ?>) field(value, 2)).values().iterator().next()).isEqualTo(expected);
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

  private static Object field(Object row, int pos) {
    return row instanceof Record record ? record.get(pos) : ((IndexedRecord) row).get(pos);
  }

  @ParameterizedTest
  @MethodSource("modes")
  void rejectsOverflow(ReaderKind reader, Type.PrimitiveType target) throws IOException {
    long units = unitsPerDay(target);
    for (int day :
        new int[] {(int) (Long.MIN_VALUE / units) - 1, (int) (Long.MAX_VALUE / units) + 1}) {
      InputFile file = write(day);
      assertThatThrownBy(() -> read(reader, schema(target), file))
          .isInstanceOf(ArithmeticException.class);
    }
  }

  @ParameterizedTest
  @MethodSource("modes")
  void rejectsZonedTargets(ReaderKind reader, Type.PrimitiveType target) throws IOException {
    Type.PrimitiveType zoned =
        target.typeId() == Type.TypeID.TIMESTAMP
            ? Types.TimestampType.withZone()
            : Types.TimestampNanoType.withZone();
    InputFile file = write(1);
    assertThatThrownBy(() -> read(reader, schema(zoned), file))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot promote date");
  }

  @ParameterizedTest
  @EnumSource(ReaderKind.class)
  void preservesDateReadsAndUnselectedColumns(ReaderKind reader) throws IOException {
    InputFile file = write(-1, 0, 1, null);
    List<Object> expected =
        reader == ReaderKind.GENERIC
            ? Arrays.asList(
                LocalDate.ofEpochDay(-1), LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(1), null)
            : Arrays.asList(-1, 0, 1, null);
    assertThat(read(reader, DATE, file)).containsExactlyElementsOf(expected);
    Schema unrelated = new Schema(Types.NestedField.optional(2, "missing", Types.StringType.get()));
    assertThat(read(reader, unrelated, file)).containsExactly(null, null, null, null);
  }

  private static List<Object> read(ReaderKind reader, Schema schema, InputFile file)
      throws IOException {
    Avro.ReadBuilder builder = Avro.read(file).project(schema).reuseContainers();
    switch (reader) {
      case GENERIC -> builder.createResolvingReader(PlannedDataReader::create);
      case INTERNAL -> builder.createResolvingReader(InternalReader::create);
      case AVRO -> builder.createResolvingReader(GenericAvroReader::create);
    }
    List<Object> values = new ArrayList<>();
    try (CloseableIterable<?> rows = builder.build()) {
      for (Object row : rows) {
        values.add(row instanceof Record record ? record.get(0) : ((IndexedRecord) row).get(0));
      }
    }
    return values;
  }

  private static InputFile write(Integer... days) throws IOException {
    InMemoryOutputFile output = new InMemoryOutputFile();
    try (FileAppender<Record> appender =
        Avro.write(output).schema(DATE).createWriterFunc(DataWriter::create).build()) {
      for (Integer day : days) {
        GenericRecord record = GenericRecord.create(DATE);
        record.set(0, day == null ? null : LocalDate.ofEpochDay(day));
        appender.add(record);
      }
    }
    return output.toInputFile();
  }

  private static Schema schema(Type type) {
    return new Schema(Types.NestedField.optional(1, "d", type));
  }

  private static long unitsPerDay(Type type) {
    return type.typeId() == Type.TypeID.TIMESTAMP ? 86_400_000_000L : 86_400_000_000_000L;
  }
}
