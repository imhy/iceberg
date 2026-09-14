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
package org.apache.iceberg.arrow.vectorized;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.arrow.ArrowAllocation;
import org.apache.iceberg.arrow.ArrowSchemaUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestDateToTimestampVectorizedRead {
  @TempDir private File temp;
  private int fileId;
  private static final Schema DATE =
      new Schema(Types.NestedField.optional(1, "d", Types.DateType.get()));

  enum Encoding {
    PLAIN,
    DICTIONARY,
    FALLBACK
  }

  static Stream<Arguments> modes() {
    return Stream.of(Types.TimestampType.withoutZone(), Types.TimestampNanoType.withoutZone())
        .flatMap(
            target ->
                Stream.of(Encoding.values())
                    .flatMap(
                        encoding ->
                            Stream.of(false, true)
                                .flatMap(
                                    reuse ->
                                        Stream.of(false, true)
                                            .map(
                                                validity ->
                                                    Arguments.of(
                                                        target, encoding, reuse, validity)))));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void convertsBatches(
      Type.PrimitiveType target, Encoding encoding, boolean reuse, boolean validity)
      throws IOException {
    List<Integer> days = new ArrayList<>();
    for (int i = 0; i < 2049; i += 1) {
      days.add(
          i % 7 == 0 ? null : encoding == Encoding.FALLBACK && i >= 500 ? i - 1500 : i % 3 - 1);
    }
    File file = write(encoding, days);
    read(file, target, encoding, reuse, validity, days);
  }

  @ParameterizedTest
  @MethodSource("modes")
  void checksTargetRange(
      Type.PrimitiveType target, Encoding encoding, boolean reuse, boolean validity)
      throws IOException {
    long units = unitsPerDay(target);
    List<Integer> boundaries =
        List.of((int) (Long.MIN_VALUE / units), (int) (Long.MAX_VALUE / units));
    read(write(encoding, boundaries), target, null, reuse, validity, boundaries);
    for (int day : new int[] {boundaries.get(0) - 1, boundaries.get(1) + 1}) {
      File file = write(encoding, List.of(day));
      assertThatThrownBy(() -> read(file, target, null, reuse, validity, List.of(day)))
          .isInstanceOf(ArithmeticException.class);
    }
  }

  static Stream<Type.PrimitiveType> zonedTargets() {
    return Stream.of(Types.TimestampType.withZone(), Types.TimestampNanoType.withZone());
  }

  @ParameterizedTest
  @MethodSource("zonedTargets")
  void rejectsZonedTargets(Type.PrimitiveType target) throws IOException {
    File file = write(Encoding.PLAIN, List.of(1));
    assertThatThrownBy(() -> read(file, target, null, false, false, List.of(1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot promote date");
  }

  private File write(Encoding encoding, List<Integer> days) throws IOException {
    File file = new File(temp, "dates-" + fileId++ + ".parquet");
    try (FileAppender<Record> writer =
        Parquet.write(Files.localOutput(file))
            .schema(DATE)
            .set("parquet.enable.dictionary", Boolean.toString(encoding != Encoding.PLAIN))
            .set(TableProperties.PARQUET_PAGE_SIZE_BYTES, "128")
            .set(TableProperties.PARQUET_PAGE_ROW_LIMIT, "100")
            .set(
                TableProperties.PARQUET_DICT_SIZE_BYTES,
                encoding == Encoding.FALLBACK ? "128" : "1048576")
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      for (Integer day : days) {
        GenericRecord row = GenericRecord.create(DATE);
        row.set(0, day == null ? null : LocalDate.ofEpochDay(day));
        writer.add(row);
      }
    }
    return file;
  }

  private static void read(
      File file,
      Type.PrimitiveType target,
      Encoding encoding,
      boolean reuse,
      boolean validity,
      List<Integer> days)
      throws IOException {
    try (BufferAllocator allocator =
            ArrowAllocation.rootAllocator().newChildAllocator("date-promotion", 0, Long.MAX_VALUE);
        ParquetFileReader parquet = ParquetFileReader.open(new LocalInputFile(file.toPath()))) {
      ColumnDescriptor desc =
          parquet.getFileMetaData().getSchema().getColumnDescription(new String[] {"d"});
      Types.NestedField field = Types.NestedField.optional(1, "d", target);
      VectorizedArrowReader reader = new VectorizedArrowReader(desc, field, allocator, validity);
      try {
        reader.setBatchSize(127);
        ColumnChunkMetaData metadata = parquet.getRowGroups().get(0).getColumns().get(0);
        if (encoding != null) {
          assertThat(ParquetUtil.hasNonDictionaryPages(metadata))
              .isEqualTo(encoding != Encoding.DICTIONARY);
          assertThat(metadata.hasDictionaryPage()).isEqualTo(encoding != Encoding.PLAIN);
        }
        try (PageReadStore pages = parquet.readNextRowGroup()) {
          reader.setRowGroupInfo(pages, Map.of(ColumnPath.get("d"), metadata));
          VectorHolder previous = null;
          for (int offset = 0; offset < days.size(); offset += 127) {
            int count = Math.min(127, days.size() - offset);
            VectorHolder holder = reader.read(reuse ? previous : null, count);
            assertThat(holder.isDictionaryEncoded()).isFalse();
            assertThat(holder.numValues()).isEqualTo(count);
            assertThat(holder.vector().getField().getType())
                .isEqualTo(ArrowSchemaUtil.convert(field).getType());
            for (int i = 0; i < count; i += 1) {
              Integer day = days.get(offset + i);
              assertThat(holder.nullabilityHolder().isNullAt(i))
                  .isEqualTo((byte) (day == null ? 1 : 0));
              if (day != null) {
                assertThat(holder.vector().getDataBuffer().getLong((long) i * Long.BYTES))
                    .isEqualTo(day * unitsPerDay(target));
              }
              if (validity) {
                assertThat(holder.vector().isNull(i)).isEqualTo(day == null);
              }
            }
            previous = holder;
          }
        }
      } finally {
        reader.close();
      }
      assertThat(allocator.getAllocatedMemory()).isZero();
    }
  }

  private static long unitsPerDay(Type type) {
    return type.typeId() == Type.TypeID.TIMESTAMP ? 86_400_000_000L : 86_400_000_000_000L;
  }
}
