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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestSinkTableFormatVersion {
  private static final Schema DATE_SCHEMA =
      new Schema(Types.NestedField.optional(1, "d", Types.DateType.get()));
  @TempDir Path temp;

  @Test
  void avoidsLoadingTablesWithKnownVersions() {
    Table table = table(3);
    TableLoader loader = mock(TableLoader.class);
    assertThat(TableUtil.formatVersion(SinkUtil.serializableTable(table, loader))).isEqualTo(3);
    verifyNoInteractions(loader);
  }

  @ParameterizedTest
  @CsvSource({"2,false", "3,false", "2,true", "3,true"})
  void preservesResolvedVersionAndWrapperState(int version, boolean serialized) throws Exception {
    Table table = table(version);
    Table wrapper = wrapper(table);
    if (serialized) {
      wrapper = SerializableTable.copyOf(wrapper);
    }
    TableLoader original = mock(TableLoader.class);
    TableLoader owned = mock(TableLoader.class);
    when(original.clone()).thenReturn(owned);
    when(owned.loadTable()).thenReturn(table);
    SerializableTable resolved = SinkUtil.serializableTable(wrapper, original);
    assertThat(resolved.io()).isSameAs(wrapper.io());
    assertThat(resolved.properties()).isEqualTo(wrapper.properties());
    assertThat(resolved.schema().sameSchema(wrapper.schema())).isTrue();
    Table copied = SerializableTable.copyOf(resolved);
    Table roundTrip =
        SerializationUtil.deserializeFromBytes(SerializationUtil.serializeToBytes(copied));
    assertThat(TableUtil.formatVersion(roundTrip)).isEqualTo(version);
    verify(owned).open();
    verify(owned).close();
    verify(original, never()).close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void buildsV3SinksForDateInput(boolean modern) {
    buildSink(table(3), modern);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectsDateInputForV2Wrappers(boolean modern) {
    assertThatThrownBy(() -> buildSink(table(2), modern))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("d");
  }

  @Test
  void writesPromotedDatesAfterSerialization() throws Exception {
    Table table = table(3);
    Table resolved =
        SinkUtil.serializableTable(wrapper(table), TableLoader.fromHadoopTable(table.location()));
    Table roundTrip =
        SerializationUtil.deserializeFromBytes(SerializationUtil.serializeToBytes(resolved));
    RowDataTaskWriterFactory factory =
        new RowDataTaskWriterFactory(
            roundTrip,
            FlinkSchemaUtil.convert(DATE_SCHEMA),
            128 * 1024 * 1024L,
            FileFormat.PARQUET,
            Map.of(),
            List.of(),
            false);
    factory.initialize(0, 0);
    DataFile file;
    try (TaskWriter<RowData> writer = factory.create()) {
      writer.write(GenericRowData.of(1));
      DataFile[] files = writer.complete().dataFiles();
      assertThat(files).hasSize(1);
      file = files[0];
    }
    try (CloseableIterable<Record> rows =
        FormatModelRegistry.readBuilder(file.format(), Record.class, table.io().newInputFile(file))
            .project(table.schema())
            .build()) {
      assertThat(rows)
          .singleElement()
          .satisfies(
              row ->
                  assertThat(row.getField("d")).isEqualTo(LocalDate.ofEpochDay(1).atStartOfDay()));
    }
  }

  @Test
  void rejectsUnresolvableVersion() throws Exception {
    Table wrapper = wrapper(table(3));
    TableLoader loader = loader(wrapper);
    assertThatThrownBy(() -> SinkUtil.serializableTable(wrapper, loader))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot resolve the format version");
    verify(loader).close();
  }

  @Test
  void rejectsDifferentTable() throws Exception {
    Table table = table(3);
    Table different =
        new HadoopTables()
            .create(
                table.schema(),
                PartitionSpec.unpartitioned(),
                Map.of("format-version", "3"),
                temp.resolve("other").toString());
    TableLoader loader = loader(different);
    assertThatThrownBy(() -> SinkUtil.serializableTable(wrapper(table), loader))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("different table");
    verify(loader).close();
  }

  @Test
  void rejectsStaleWrapperSchema() throws Exception {
    Table table = table(3);
    Table wrapper = SerializableTable.copyOf(wrapper(table));
    table.updateSchema().addColumn("extra", Types.IntegerType.get()).commit();
    TableLoader loader = loader(table);
    assertThatThrownBy(() -> SinkUtil.serializableTable(wrapper, loader))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("schema differs");
    verify(loader).close();
  }

  @Test
  void propagatesMetadataFailureWithoutFallback() {
    Table table = mock(Table.class, withSettings().extraInterfaces(HasTableOperations.class));
    when(((HasTableOperations) table).operations())
        .thenThrow(new IllegalArgumentException("Broken metadata"));
    TableLoader loader = mock(TableLoader.class);
    assertThatThrownBy(() -> SinkUtil.serializableTable(table, loader))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Broken metadata");
    verifyNoInteractions(loader);
  }

  @Test
  void closesLoaderAfterLoadFailure() throws Exception {
    TableLoader loader = loader(null);
    when(loader.loadTable()).thenThrow(new IllegalStateException("Failed to load"));
    assertThatThrownBy(() -> SinkUtil.serializableTable(wrapper(table(3)), loader))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Failed to load");
    verify(loader).close();
  }

  private void buildSink(Table table, boolean modern) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var stream =
        env.fromCollection(
            List.<RowData>of(GenericRowData.of(1)),
            InternalTypeInfo.of(FlinkSchemaUtil.convert(DATE_SCHEMA)));
    ResolvedSchema schema =
        ResolvedSchema.physical(new String[] {"d"}, new DataType[] {DataTypes.DATE()});
    TableLoader loader = TableLoader.fromHadoopTable(table.location());
    if (modern) {
      IcebergSink.forRowData(stream)
          .resolvedSchema(schema)
          .table(wrapper(table))
          .tableLoader(loader)
          .append();
    } else {
      FlinkSink.forRowData(stream)
          .resolvedSchema(schema)
          .table(wrapper(table))
          .tableLoader(loader)
          .append();
    }
  }

  private Table table(int version) {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "d", Types.TimestampType.withoutZone()));
    return new HadoopTables()
        .create(
            schema,
            PartitionSpec.unpartitioned(),
            Map.of("format-version", Integer.toString(version)),
            temp.resolve("t").toString());
  }

  private static TableLoader loader(Table table) {
    TableLoader loader = mock(TableLoader.class);
    when(loader.clone()).thenReturn(loader);
    when(loader.loadTable()).thenReturn(table);
    return loader;
  }

  private static Table wrapper(Table table) {
    return (Table)
        Proxy.newProxyInstance(
            Table.class.getClassLoader(),
            new Class<?>[] {Table.class},
            (proxy, method, args) -> {
              try {
                return method.invoke(table, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
  }
}
