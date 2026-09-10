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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.data.parquet.InternalReader;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestDateToTimestampRead {
  private static final Schema DATE_SCHEMA = schema(Types.DateType.get());
  @TempDir private File temp;

  static Stream<Arguments> readModes() {
    return Stream.of(Types.TimestampType.withoutZone(), Types.TimestampNanoType.withoutZone())
        .flatMap(type -> Stream.of(Arguments.of(type, true), Arguments.of(type, false)));
  }

  @ParameterizedTest
  @MethodSource("readModes")
  void rowGroupFilters(Type.PrimitiveType type, boolean dictionary) throws IOException {
    InputFile file = write(dictionary, -1, 1, -1, 1);
    Schema expected = schema(type);
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(file))) {
      assertThat(reader.getRowGroups()).hasSize(1);
      BlockMetaData rowGroup = reader.getRowGroups().get(0);
      MessageType fileSchema = reader.getFileMetaData().getSchema();
      assertThat(ParquetUtil.hasNonDictionaryPages(rowGroup.getColumns().get(0)))
          .isEqualTo(!dictionary);
      if (!dictionary) {
        // Parquet may omit bloom filters for columns encoded entirely with dictionaries.
        assertThat(
                reader
                    .getBloomFilterDataReader(rowGroup)
                    .readBloomFilter(rowGroup.getColumns().get(0)))
            .isNotNull();
      }
      for (Expression expr :
          List.of(
              Expressions.equal("d", "1970-01-02T00:00:00"),
              Expressions.equal("d", "1969-12-31T00:00:00"),
              Expressions.in("d", "1969-12-31T00:00:00", "1970-01-02T00:00:00"),
              Expressions.greaterThanOrEqual("d", "1970-01-02T00:00:00"))) {
        assertThat(
                new ParquetMetricsRowGroupFilter(expected, expr).shouldRead(fileSchema, rowGroup))
            .isTrue();
        assertThat(
                new ParquetDictionaryRowGroupFilter(expected, expr)
                    .shouldRead(fileSchema, rowGroup, reader.getDictionaryReader(rowGroup)))
            .isTrue();
        assertThat(
                new ParquetBloomRowGroupFilter(expected, expr)
                    .shouldRead(fileSchema, rowGroup, reader.getBloomFilterDataReader(rowGroup)))
            .isTrue();
      }
      Expression disjoint = Expressions.equal("d", "1970-01-03T00:00:00");
      assertThat(
              new ParquetMetricsRowGroupFilter(expected, disjoint).shouldRead(fileSchema, rowGroup))
          .isFalse();
      // Epoch zero is inside the stats range but absent from the dictionary.
      Expression gap = Expressions.equal("d", "1970-01-01T00:00:00");
      assertThat(
              new ParquetDictionaryRowGroupFilter(expected, gap)
                  .shouldRead(fileSchema, rowGroup, reader.getDictionaryReader(rowGroup)))
          .isEqualTo(!dictionary);
    }
  }

  @ParameterizedTest
  @MethodSource("readModes")
  void internalReaderPromotesDate(Type.PrimitiveType type, boolean dictionary) throws IOException {
    InputFile file = write(dictionary, -1, 0, 1, null);
    try (CloseableIterable<Record> rows =
        Parquet.read(file)
            .project(schema(type))
            .createReaderFunc(fileSchema -> InternalReader.create(schema(type), fileSchema))
            .reuseContainers()
            .build()) {
      long unitsPerDay = unitsPerDay(type);
      assertThat(rows)
          .extracting(row -> row.get(0, Long.class))
          .containsExactly(-unitsPerDay, 0L, unitsPerDay, null);
    }
  }

  @ParameterizedTest
  @MethodSource("readModes")
  void internalReaderRejectsOverflow(Type.PrimitiveType type, boolean dictionary)
      throws IOException {
    InputFile file = write(dictionary, (int) (Long.MIN_VALUE / unitsPerDay(type)) - 1);
    assertThatThrownBy(
            () -> {
              try (CloseableIterable<Record> rows =
                  Parquet.read(file)
                      .project(schema(type))
                      .createReaderFunc(
                          fileSchema -> InternalReader.create(schema(type), fileSchema))
                      .build()) {
                rows.iterator().next();
              }
            })
        .isInstanceOf(ArithmeticException.class);
  }

  @ParameterizedTest
  @MethodSource("readModes")
  void rejectsZonedDatePromotion(Type.PrimitiveType type, boolean dictionary) throws IOException {
    InputFile file = write(dictionary, 1);
    Type.PrimitiveType zoned =
        type.typeId() == Type.TypeID.TIMESTAMP
            ? Types.TimestampType.withZone()
            : Types.TimestampNanoType.withZone();
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(file))) {
      MessageType fileSchema = reader.getFileMetaData().getSchema();
      assertThatThrownBy(() -> GenericParquetReaders.buildReader(schema(zoned), fileSchema))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> InternalReader.create(schema(zoned), fileSchema))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  private InputFile write(boolean dictionary, Integer... days) throws IOException {
    File file = new File(temp, "dates.parquet");
    try (FileAppender<Record> appender =
        Parquet.write(Files.localOutput(file))
            .schema(DATE_SCHEMA)
            .set("parquet.enable.dictionary", Boolean.toString(dictionary))
            .set(TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "d", "true")
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      for (Integer day : days) {
        GenericRecord record = GenericRecord.create(DATE_SCHEMA);
        record.setField("d", day == null ? null : LocalDate.ofEpochDay(day));
        appender.add(record);
      }
    }
    return Files.localInput(file);
  }

  private static Schema schema(Type.PrimitiveType type) {
    return new Schema(Types.NestedField.optional(1, "d", type));
  }

  private static long unitsPerDay(Type type) {
    return type.typeId() == Type.TypeID.TIMESTAMP ? 86_400_000_000L : 86_400_000_000_000L;
  }
}
