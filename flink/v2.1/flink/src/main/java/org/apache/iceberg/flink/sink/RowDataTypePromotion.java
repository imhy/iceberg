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

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.iceberg.flink.FlinkRowData;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/** Normalizes promoted DATE fields while retaining the input representation of other fields. */
final class RowDataTypePromotion {
  private RowDataTypePromotion() {}

  static LogicalType promotedType(LogicalType source, Type target) {
    if (target == null) {
      return source;
    }
    switch (source.getTypeRoot()) {
      case DATE:
        if (target.isPrimitiveType()
            && TypeUtil.isDateToTimestampPromotion(
                Types.DateType.get(), target.asPrimitiveType())) {
          return FlinkSchemaUtil.convert(target).copy(source.isNullable());
        }
        return source;
      case ROW:
        if (!target.isStructType()) {
          return source;
        }
        List<RowType.RowField> fields = Lists.newArrayList();
        for (RowType.RowField field : ((RowType) source).getFields()) {
          Types.NestedField targetField = target.asStructType().field(field.getName());
          fields.add(
              new RowType.RowField(
                  field.getName(),
                  promotedType(field.getType(), targetField == null ? null : targetField.type()),
                  field.getDescription().orElse(null)));
        }
        return new RowType(source.isNullable(), fields);
      case ARRAY:
        return target.isListType()
            ? new ArrayType(
                source.isNullable(),
                promotedType(
                    ((ArrayType) source).getElementType(), target.asListType().elementType()))
            : source;
      case MAP:
        return target.isMapType()
            ? new MapType(
                source.isNullable(),
                promotedType(((MapType) source).getKeyType(), target.asMapType().keyType()),
                promotedType(((MapType) source).getValueType(), target.asMapType().valueType()))
            : source;
      default:
        return source;
    }
  }

  static TaskWriter<RowData> wrap(TaskWriter<RowData> writer, RowType source, RowType target) {
    if (source.equals(target)) {
      return writer;
    }
    Function<Object, Object> convert = converter(source, target);
    return new TaskWriter<>() {
      @Override
      public void write(RowData row) throws IOException {
        writer.write((RowData) convert.apply(row));
      }

      @Override
      public void abort() throws IOException {
        writer.abort();
      }

      @Override
      public WriteResult complete() throws IOException {
        return writer.complete();
      }

      @Override
      public void close() throws IOException {
        writer.close();
      }
    };
  }

  private static Function<Object, Object> converter(LogicalType source, LogicalType target) {
    if (source.equals(target)) {
      return Function.identity();
    }
    Function<Object, Object> convert;
    switch (source.getTypeRoot()) {
      case DATE:
        ChronoUnit unit =
            ((TimestampType) target).getPrecision() > 6 ? ChronoUnit.NANOS : ChronoUnit.MICROS;
        convert = value -> RowDataUtil.timestampFromDays((Integer) value, unit);
        break;
      case ROW:
        RowType fromRow = (RowType) source;
        RowType toRow = (RowType) target;
        int[] positions = new int[fromRow.getFieldCount()];
        Arrays.fill(positions, -1);
        List<RowData.FieldGetter> getters = Lists.newArrayList();
        List<Function<Object, Object>> conversions = Lists.newArrayList();
        for (int i = 0; i < fromRow.getFieldCount(); i++) {
          if (!fromRow.getTypeAt(i).equals(toRow.getTypeAt(i))) {
            positions[i] = getters.size();
            getters.add(FlinkRowData.createFieldGetter(fromRow.getTypeAt(i), i));
            conversions.add(converter(fromRow.getTypeAt(i), toRow.getTypeAt(i)));
          }
        }
        convert =
            value -> {
              RowData row = (RowData) value;
              Object[] values = new Object[getters.size()];
              for (int i = 0; i < values.length; i++) {
                values[i] = conversions.get(i).apply(getters.get(i).getFieldOrNull(row));
              }
              return new PromotedRowData(row, positions, values);
            };
        break;
      case ARRAY:
        ArrayType fromArray = (ArrayType) source;
        ArrayData.ElementGetter getter = ArrayData.createElementGetter(fromArray.getElementType());
        Function<Object, Object> element =
            converter(fromArray.getElementType(), ((ArrayType) target).getElementType());
        convert =
            value -> {
              ArrayData array = (ArrayData) value;
              Object[] result = new Object[array.size()];
              for (int i = 0; i < result.length; i++) {
                result[i] = element.apply(elementAt(array, getter, i));
              }
              return new GenericArrayData(result);
            };
        break;
      case MAP:
        MapType fromMap = (MapType) source;
        MapType toMap = (MapType) target;
        ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(fromMap.getKeyType());
        ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(fromMap.getValueType());
        Function<Object, Object> key = converter(fromMap.getKeyType(), toMap.getKeyType());
        Function<Object, Object> mapValue = converter(fromMap.getValueType(), toMap.getValueType());
        convert =
            value -> {
              MapData map = (MapData) value;
              ArrayData keys = map.keyArray();
              ArrayData values = map.valueArray();
              Map<Object, Object> result = new LinkedHashMap<>();
              for (int i = 0; i < map.size(); i++) {
                result.put(
                    key.apply(elementAt(keys, keyGetter, i)),
                    mapValue.apply(elementAt(values, valueGetter, i)));
              }
              return new GenericMapData(result);
            };
        break;
      default:
        throw new IllegalArgumentException("Cannot promote input type: " + source);
    }
    return value -> value == null ? null : convert.apply(value);
  }

  private static Object elementAt(ArrayData array, ArrayData.ElementGetter getter, int pos) {
    return array.isNullAt(pos) ? null : getter.getElementOrNull(array, pos);
  }
}
