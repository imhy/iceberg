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
package org.apache.iceberg.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestDateToTimestampTaskWriter {
  @TempDir Path temp;

  static Stream<Type.PrimitiveType> targets() {
    return Stream.of(Types.TimestampType.withoutZone(), Types.TimestampNanoType.withoutZone());
  }

  static Stream<Arguments> modes() {
    return targets()
        .flatMap(
            target ->
                Stream.of(FileFormat.PARQUET, FileFormat.AVRO, FileFormat.ORC)
                    .flatMap(
                        format ->
                            Stream.of("none", "day", "hour")
                                .map(partition -> Arguments.of(target, format, partition))));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void writesDatesUsingTheTableRepresentation(
      Type.PrimitiveType target, FileFormat format, String partition) throws Exception {
    Schema schema = schema(target);
    PartitionSpec spec = spec(schema, partition);
    Table table =
        new HadoopTables(new Configuration())
            .create(schema, spec, Map.of("format-version", "3"), temp.resolve("table").toString());
    RowDataTaskWriterFactory factory =
        new RowDataTaskWriterFactory(
            SerializableTable.copyOf(table),
            FlinkSchemaUtil.convert(schema(Types.DateType.get())),
            Long.MAX_VALUE,
            format,
            Map.of(),
            null,
            false);
    // Exercise the same serialization boundary as an engine task.
    factory = SerializationUtil.deserializeFromBytes(SerializationUtil.serializeToBytes(factory));
    factory.initialize(0, 0);
    DataFile[] files;
    try (TaskWriter<RowData> writer = factory.create()) {
      int id = 0;
      for (Integer days : Arrays.asList(-1, 0, 1, null)) {
        writer.write(GenericRowData.of(++id, days));
      }
      files = writer.complete().dataFiles();
    }
    AppendFiles append = table.newAppend();
    for (DataFile file : files) {
      append.appendFile(file);
    }
    append.commit();
    List<Object> values = Lists.newArrayList();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      for (Record row : rows) {
        values.add(row.getField("d"));
      }
    }
    assertThat(values)
        .containsExactlyInAnyOrder(
            LocalDate.ofEpochDay(-1).atStartOfDay(),
            LocalDate.ofEpochDay(0).atStartOfDay(),
            LocalDate.ofEpochDay(1).atStartOfDay(),
            null);
    if (!spec.isUnpartitioned()) {
      int units = partition.equals("hour") ? 24 : 1;
      assertThat(files)
          .extracting(file -> file.partition().get(0, Integer.class))
          .containsExactlyInAnyOrder(-units, 0, units, null);
    }
  }

  @ParameterizedTest
  @MethodSource("targets")
  void routesDatesLikeTheirPromotedValues(Type.PrimitiveType target) {
    Schema schema = schema(target);
    RowType dateType = FlinkSchemaUtil.convert(schema(Types.DateType.get()));
    RowType timestampType = FlinkSchemaUtil.convert(schema);
    for (String partition : List.of("day", "hour")) {
      PartitionSpec spec = spec(schema, partition);
      PartitionKeySelector dates = new PartitionKeySelector(spec, schema, dateType);
      PartitionKeySelector timestamps = new PartitionKeySelector(spec, schema, timestampType);
      EqualityFieldKeySelector dateKeys = new EqualityFieldKeySelector(schema, dateType, Set.of(2));
      EqualityFieldKeySelector timestampKeys =
          new EqualityFieldKeySelector(schema, timestampType, Set.of(2));
      for (Integer days : Arrays.asList(-1, 0, 1, null)) {
        RowData date = GenericRowData.of(1, days);
        RowData timestamp =
            GenericRowData.of(
                1,
                days == null
                    ? null
                    : TimestampData.fromLocalDateTime(LocalDate.ofEpochDay(days).atStartOfDay()));
        assertThat(dates.getKey(date)).isEqualTo(timestamps.getKey(timestamp));
        assertThat(dateKeys.getKey(date)).isEqualTo(timestampKeys.getKey(timestamp));
        SortOrder order = SortOrder.builderFor(schema).asc("d").build();
        SortKey dateSort = new SortKey(schema, order);
        SortKey timestampSort = new SortKey(schema, order);
        dateSort.wrap(new RowDataWrapper(dateType, schema.asStruct()).wrap(date));
        timestampSort.wrap(new RowDataWrapper(timestampType, schema.asStruct()).wrap(timestamp));
        assertThat(dateSort.get(0, Long.class)).isEqualTo(timestampSort.get(0, Long.class));
      }
    }
  }

  static Stream<Arguments> formats() {
    return targets()
        .flatMap(
            target ->
                Stream.of(FileFormat.PARQUET, FileFormat.AVRO, FileFormat.ORC)
                    .map(format -> Arguments.of(target, format)));
  }

  @ParameterizedTest
  @MethodSource("targets")
  void gatesRequestedSchemasByTableVersion(Type.PrimitiveType target) {
    Schema schema = schema(target);
    RowType dates = FlinkSchemaUtil.convert(schema(Types.DateType.get()));
    for (int version : List.of(1, 2)) {
      assertThatThrownBy(
              () ->
                  FlinkSink.toFlinkRowType(
                      version, schema, FlinkSchemaUtil.toResolvedSchema(dates)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("date cannot be promoted to timestamp");
      assertThatThrownBy(
              () -> FlinkSink.toFlinkRowType(version, schema, FlinkSchemaUtil.toSchema(dates)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("date cannot be promoted to timestamp");
    }
    for (int version : List.of(3, 4)) {
      assertThat(
              FlinkSink.toFlinkRowType(version, schema, FlinkSchemaUtil.toResolvedSchema(dates))
                  .getFields())
          .isEqualTo(dates.getFields());
      assertThat(
              FlinkSink.toFlinkRowType(version, schema, FlinkSchemaUtil.toSchema(dates))
                  .getFields())
          .isEqualTo(dates.getFields());
    }
  }

  @ParameterizedTest
  @MethodSource("targets")
  void rejectsValuesOutsideTheTargetRange(Type.PrimitiveType target) throws Exception {
    Schema schema = schema(target);
    Table table =
        new HadoopTables(new Configuration())
            .create(
                schema,
                PartitionSpec.unpartitioned(),
                Map.of("format-version", "3"),
                temp.resolve("table").toString());
    RowDataTaskWriterFactory factory =
        new RowDataTaskWriterFactory(
            SerializableTable.copyOf(table),
            FlinkSchemaUtil.convert(schema(Types.DateType.get())),
            Long.MAX_VALUE,
            FileFormat.PARQUET,
            Map.of(),
            null,
            false);
    factory.initialize(0, 0);
    long units = target instanceof Types.TimestampNanoType ? 86_400_000_000_000L : 86_400_000_000L;
    try (TaskWriter<RowData> writer = factory.create()) {
      for (int days :
          new int[] {(int) (Long.MAX_VALUE / units) + 1, (int) (Long.MIN_VALUE / units) - 1}) {
        assertThatThrownBy(() -> writer.write(GenericRowData.of(1, days)))
            .isInstanceOf(ArithmeticException.class)
            .hasMessage("long overflow");
      }
      writer.abort();
    }
  }

  @ParameterizedTest
  @MethodSource("formats")
  void preservesDeletesAndUpserts(Type.PrimitiveType target, FileFormat format) throws Exception {
    for (boolean upsert : List.of(false, true)) {
      Schema schema = schema(target);
      Table table =
          new HadoopTables(new Configuration())
              .create(
                  schema,
                  PartitionSpec.unpartitioned(),
                  Map.of("format-version", "3"),
                  temp.resolve("table-" + upsert).toString());
      RowDataTaskWriterFactory factory =
          new RowDataTaskWriterFactory(
              SerializableTable.copyOf(table),
              FlinkSchemaUtil.convert(schema(Types.DateType.get())),
              Long.MAX_VALUE,
              format,
              Map.of(),
              Set.of(1),
              upsert);
      factory.initialize(0, 0);
      WriteResult result;
      try (TaskWriter<RowData> writer = factory.create()) {
        writer.write(row(RowKind.INSERT, 1, 1));
        writer.write(row(RowKind.UPDATE_BEFORE, 1, 1));
        writer.write(row(RowKind.UPDATE_AFTER, 1, 2));
        writer.write(row(RowKind.INSERT, 2, 3));
        writer.write(row(RowKind.DELETE, 2, 3));
        result = writer.complete();
      }
      RowDelta delta = table.newRowDelta();
      for (DataFile file : result.dataFiles()) {
        delta.addRows(file);
      }
      for (DeleteFile file : result.deleteFiles()) {
        delta.addDeletes(file);
      }
      delta.commit();
      try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
        assertThat(rows)
            .singleElement()
            .satisfies(
                record -> {
                  assertThat(record.getField("id")).isEqualTo(1);
                  assertThat(record.getField("d"))
                      .isEqualTo(LocalDate.ofEpochDay(2).atStartOfDay());
                });
      }
    }
  }

  @ParameterizedTest
  @MethodSource("formats")
  void convertsNestedDatesAndRetainsOtherInputTypes(Type.PrimitiveType target, FileFormat format)
      throws Exception {
    Schema schema = nestedSchema(target);
    RowType source = FlinkSchemaUtil.convert(nestedSchema(Types.DateType.get()));
    List<RowType.RowField> inputFields = Lists.newArrayList(source.getFields());
    inputFields.set(0, new RowType.RowField("id", new TinyIntType(false)));
    RowType input = new RowType(source.isNullable(), inputFields);
    Table table =
        new HadoopTables(new Configuration())
            .create(
                schema,
                PartitionSpec.unpartitioned(),
                Map.of("format-version", "3"),
                temp.resolve("table").toString());
    RowDataTaskWriterFactory factory =
        new RowDataTaskWriterFactory(
            SerializableTable.copyOf(table), input, Long.MAX_VALUE, format, Map.of(), null, false);
    factory.initialize(0, 0);
    WriteResult result;
    try (TaskWriter<RowData> writer = factory.create()) {
      writer.write(
          GenericRowData.of(
              (byte) 7,
              GenericRowData.of(1),
              new GenericArrayData(new Integer[] {0, null, -1}),
              new GenericMapData(Map.of(2, 1))));
      writer.write(GenericRowData.of((byte) 8, null, null, null));
      result = writer.complete();
    }
    AppendFiles append = table.newAppend();
    for (DataFile file : result.dataFiles()) {
      append.appendFile(file);
    }
    append.commit();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      List<Record> actual = Lists.newArrayList();
      rows.forEach(actual::add);
      assertThat(actual).hasSize(2);
      Record present =
          actual.stream().filter(r -> r.getField("id").equals(7)).findFirst().orElseThrow();
      assertThat(((Record) present.getField("s")).getField("d"))
          .isEqualTo(LocalDate.ofEpochDay(1).atStartOfDay());
      assertThat(present.getField("l"))
          .asList()
          .containsExactly(
              LocalDate.ofEpochDay(0).atStartOfDay(),
              null,
              LocalDate.ofEpochDay(-1).atStartOfDay());
      assertThat(((Map<?, ?>) present.getField("m")).get(2))
          .isEqualTo(LocalDate.ofEpochDay(1).atStartOfDay());
      Record absent =
          actual.stream().filter(r -> r.getField("id").equals(8)).findFirst().orElseThrow();
      assertThat(absent.getField("s")).isNull();
      assertThat(absent.getField("l")).isNull();
      assertThat(absent.getField("m")).isNull();
    }
  }

  @ParameterizedTest
  @MethodSource("targets")
  void retainsNullFlagsOnRequiredInputFields(Type.PrimitiveType target) throws Exception {
    Schema schema = schema(target);
    Table table =
        new HadoopTables(new Configuration())
            .create(
                schema,
                PartitionSpec.unpartitioned(),
                Map.of("format-version", "3"),
                temp.resolve("table").toString());
    RowDataTaskWriterFactory factory =
        new RowDataTaskWriterFactory(
            SerializableTable.copyOf(table),
            FlinkSchemaUtil.convert(schema(Types.DateType.get())),
            Long.MAX_VALUE,
            FileFormat.PARQUET,
            Map.of(),
            null,
            false);
    factory.initialize(0, 0);
    BinaryRowData row = new BinaryRowData(2);
    BinaryRowWriter binary = new BinaryRowWriter(row);
    binary.writeInt(0, 123);
    binary.setNullAt(0);
    binary.writeInt(1, 1);
    binary.complete();
    try (TaskWriter<RowData> writer = factory.create()) {
      assertThatThrownBy(() -> writer.write(row))
          .isInstanceOf(NullPointerException.class)
          .hasMessage("Cannot invoke \"java.lang.Integer.intValue()\" because \"value\" is null");
      writer.abort();
    }
  }

  private static Schema nestedSchema(Type type) {
    return new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(
            2, "s", Types.StructType.of(Types.NestedField.optional(3, "d", type))),
        Types.NestedField.optional(4, "l", Types.ListType.ofOptional(5, type)),
        Types.NestedField.optional(
            6, "m", Types.MapType.ofOptional(7, 8, Types.IntegerType.get(), type)));
  }

  private static RowData row(RowKind kind, int id, int days) {
    GenericRowData row = GenericRowData.of(id, days);
    row.setRowKind(kind);
    return row;
  }

  private static Schema schema(Type type) {
    return new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "d", type));
  }

  private static PartitionSpec spec(Schema schema, String partition) {
    return switch (partition) {
      case "day" -> PartitionSpec.builderFor(schema).day("d").build();
      case "hour" -> PartitionSpec.builderFor(schema).hour("d").build();
      default -> PartitionSpec.unpartitioned();
    };
  }
}
