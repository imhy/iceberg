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
package org.apache.iceberg.spark.data;

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
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkOrcReaders;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestDateToTimestampRead {
  @TempDir private File temp;
  private int fileId;
  private static final Schema DATE = schema(Types.DateType.get());

  static Stream<Arguments> modes() {
    return Stream.of(Types.TimestampType.withoutZone())
        .flatMap(
            target ->
                Stream.of(
                    Arguments.of(FileFormat.AVRO, false, target),
                    Arguments.of(FileFormat.ORC, false, target),
                    Arguments.of(FileFormat.ORC, true, target),
                    Arguments.of(FileFormat.PARQUET, false, target),
                    Arguments.of(FileFormat.PARQUET, true, target)));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void readsDatesAsTimestamps(
      FileFormat format, boolean vectorizedOrDictionary, Type.PrimitiveType target)
      throws IOException {
    long units = unitsPerDay(target);
    Integer[] days = {
      (int) (Long.MIN_VALUE / units), -1, 0, 1, (int) (Long.MAX_VALUE / units), null
    };
    InputFile file = write(format, vectorizedOrDictionary, days);
    List<Object> expected = new ArrayList<>();
    for (Integer day : days) {
      expected.add(day == null ? null : day * units);
    }
    assertThat(read(format, vectorizedOrDictionary, file, schema(target)))
        .containsExactlyElementsOf(expected);
  }

  @ParameterizedTest
  @MethodSource("modes")
  void promotesNestedFields(
      FileFormat format, boolean vectorizedOrDictionary, Type.PrimitiveType target)
      throws IOException {
    Schema original = nestedSchema(Types.DateType.get());
    Schema promoted = nestedSchema(target);
    LocalDate day = LocalDate.ofEpochDay(-1);
    GenericRecord nested = GenericRecord.create(original.findType("s").asStructType());
    nested.set(0, day);
    nested.set(1, Arrays.asList(day, null));
    nested.set(2, Map.of("key", day));
    GenericRecord root = GenericRecord.create(original);
    root.set(0, nested);
    File file = new File(temp, "nested." + format);
    FileAppender<Record> writer =
        newWriter(format, vectorizedOrDictionary, Files.localOutput(file), original);
    try (writer) {
      writer.add(root);
    }
    CloseableIterable<InternalRow> reader =
        newReader(format, vectorizedOrDictionary, Files.localInput(file), promoted);
    Object expected = -unitsPerDay(target);

    try (reader) {
      InternalRow actual = reader.iterator().next().getStruct(0, 3);
      assertThat(actual.getLong(0)).isEqualTo(expected);
      assertThat(actual.getArray(1).getLong(0)).isEqualTo(expected);
      assertThat(actual.getArray(1).isNullAt(1)).isTrue();
      assertThat(actual.getMap(2).valueArray().getLong(0)).isEqualTo(expected);
    }
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

  @ParameterizedTest
  @MethodSource("modes")
  void rejectsNanosecondTargets(
      FileFormat format, boolean vectorizedOrDictionary, Type.PrimitiveType target)
      throws IOException {
    InputFile file = write(format, vectorizedOrDictionary, 1);
    assertThatThrownBy(
            () ->
                read(
                    format,
                    vectorizedOrDictionary,
                    file,
                    schema(Types.TimestampNanoType.withoutZone())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot promote date to Spark type");
  }

  @ParameterizedTest
  @MethodSource("modes")
  void rejectsOverflow(FileFormat format, boolean vectorizedOrDictionary, Type.PrimitiveType target)
      throws IOException {
    long units = unitsPerDay(target);
    for (int day :
        new int[] {(int) (Long.MIN_VALUE / units) - 1, (int) (Long.MAX_VALUE / units) + 1}) {
      InputFile file = write(format, vectorizedOrDictionary, day);
      assertThatThrownBy(() -> read(format, vectorizedOrDictionary, file, schema(target)))
          .isInstanceOf(ArithmeticException.class);
    }
  }

  @ParameterizedTest
  @MethodSource("modes")
  void rejectsZonedTargets(
      FileFormat format, boolean vectorizedOrDictionary, Type.PrimitiveType target)
      throws IOException {
    Type.PrimitiveType zoned =
        target.typeId() == Type.TypeID.TIMESTAMP
            ? Types.TimestampType.withZone()
            : Types.TimestampNanoType.withZone();
    InputFile file = write(format, vectorizedOrDictionary, 1);
    assertThatThrownBy(() -> read(format, vectorizedOrDictionary, file, schema(zoned)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            format == FileFormat.ORC ? "Can not promote DATE" : "Cannot promote date");
  }

  @ParameterizedTest
  @MethodSource("modes")
  void preservesDateReads(
      FileFormat format, boolean vectorizedOrDictionary, Type.PrimitiveType target)
      throws IOException {
    InputFile file = write(format, vectorizedOrDictionary, -1, 0, 1, null);
    assertThat(read(format, vectorizedOrDictionary, file, DATE)).containsExactly(-1, 0, 1, null);
    Schema missing = new Schema(Types.NestedField.optional(2, "missing", target));
    assertThat(read(format, vectorizedOrDictionary, file, missing))
        .containsExactly(null, null, null, null);
  }

  private List<Object> read(
      FileFormat format, boolean vectorizedOrDictionary, InputFile file, Schema schema)
      throws IOException {
    CloseableIterable<InternalRow> reader = newReader(format, vectorizedOrDictionary, file, schema);
    List<Object> values = new ArrayList<>();
    try (reader) {
      for (InternalRow row : reader) {
        values.add(
            row.isNullAt(0)
                ? null
                : schema.columns().get(0).type().typeId() == Type.TypeID.DATE
                    ? (Object) row.getInt(0)
                    : row.getLong(0));
      }
    }
    return values;
  }

  private InputFile write(FileFormat format, boolean vectorizedOrDictionary, Integer... days)
      throws IOException {
    File file = new File(temp, "dates-" + fileId++ + "." + format);
    FileAppender<Record> writer =
        newWriter(format, vectorizedOrDictionary, Files.localOutput(file), DATE);
    try (writer) {
      for (Integer day : days) {
        GenericRecord record = GenericRecord.create(DATE);
        record.set(0, day == null ? null : LocalDate.ofEpochDay(day));
        writer.add(record);
      }
    }
    return Files.localInput(file);
  }

  private static CloseableIterable<InternalRow> newReader(
      FileFormat format, boolean vectorizedOrDictionary, InputFile file, Schema schema)
      throws IOException {
    if (format == FileFormat.ORC) {
      if (vectorizedOrDictionary) {
        CloseableIterable<ColumnarBatch> batches =
            ORC.read(file)
                .project(schema)
                .recordsPerBatch(2)
                .createBatchedReaderFunc(
                    s -> VectorizedSparkOrcReaders.buildReader(schema, s, Map.of()))
                .build();
        CloseableIterable<InternalRow> rows =
            CloseableIterable.concat(
                CloseableIterable.transform(
                    batches,
                    b -> CloseableIterable.withNoopClose((Iterable<InternalRow>) b::rowIterator)));
        return CloseableIterable.combine(
            rows,
            () -> {
              try (rows;
                  batches) {
                // Close both the flattened iterator and its ORC input.
              }
            });
      }
      return ORC.read(file)
          .project(schema)
          .recordsPerBatch(2)
          .createReaderFunc(s -> new SparkOrcReader(schema, s))
          .build();
    } else if (format == FileFormat.PARQUET) {
      return Parquet.read(file)
          .project(schema)
          .createReaderFunc(s -> SparkParquetReaders.buildReader(schema, s))
          .build();
    } else {
      return Avro.read(file)
          .project(schema)
          .createResolvingReader(SparkPlannedAvroReader::create)
          .build();
    }
  }

  private static FileAppender<Record> newWriter(
      FileFormat format, boolean vectorizedOrDictionary, OutputFile file, Schema schema)
      throws IOException {
    if (format == FileFormat.ORC) {
      return ORC.write(file).schema(schema).createWriterFunc(GenericOrcWriter::buildWriter).build();
    } else if (format == FileFormat.PARQUET) {
      return Parquet.write(file)
          .schema(schema)
          .set("parquet.enable.dictionary", Boolean.toString(vectorizedOrDictionary))
          .createWriterFunc(GenericParquetWriter::create)
          .build();
    } else {
      return Avro.write(file).schema(schema).createWriterFunc(DataWriter::create).build();
    }
  }

  private static Schema schema(Type type) {
    return new Schema(Types.NestedField.optional(1, "d", type));
  }

  private static long unitsPerDay(Type type) {
    return type.typeId() == Type.TypeID.TIMESTAMP ? 86_400_000_000L : 86_400_000_000_000L;
  }
}
