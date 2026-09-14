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
package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.spark.functions.DateToTimestampNtzFunction.DateToTimestampNtz;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.distributions.ClusteredDistribution;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.distributions.OrderedDistribution;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Keeps raw input conversion consistent with write partitioning and ordering. */
final class SparkTypePromotion {
  private SparkTypePromotion() {}

  static StructType writeType(StructType sparkType, Schema input, Schema target) {
    return (StructType) promoteType(sparkType, input.asStruct(), target.asStruct());
  }

  private static DataType promoteType(DataType sparkType, Type input, Type target) {
    if (target == null) {
      return sparkType;
    }
    if (input.isPrimitiveType()
        && target.isPrimitiveType()
        && TypeUtil.isDateToTimestampPromotion(input, target.asPrimitiveType())) {
      if (!Types.TimestampType.withoutZone().equals(target)) {
        throw new UnsupportedOperationException("Spark does not support timestamp_ns");
      }
      return DataTypes.TimestampNTZType;
    }
    if (sparkType instanceof StructType struct && target.isStructType()) {
      StructField[] fields = struct.fields().clone();
      for (int i = 0; i < fields.length; i++) {
        Types.NestedField from = input.asStructType().fields().get(i);
        Types.NestedField to = target.asStructType().field(from.fieldId());
        StructField field = fields[i];
        fields[i] =
            new StructField(
                field.name(),
                promoteType(field.dataType(), from.type(), to == null ? null : to.type()),
                field.nullable(),
                field.metadata());
      }
      return new StructType(fields);
    } else if (sparkType instanceof ArrayType array && target.isListType()) {
      return DataTypes.createArrayType(
          promoteType(
              array.elementType(),
              input.asListType().elementType(),
              target.asListType().elementType()),
          array.containsNull());
    } else if (sparkType instanceof MapType map && target.isMapType()) {
      return DataTypes.createMapType(
          promoteType(map.keyType(), input.asMapType().keyType(), target.asMapType().keyType()),
          promoteType(
              map.valueType(), input.asMapType().valueType(), target.asMapType().valueType()),
          map.valueContainsNull());
    }
    return sparkType;
  }

  static Distribution distribution(Distribution distribution, Schema input, Schema target) {
    if (distribution instanceof ClusteredDistribution clustered) {
      return Distributions.clustered(
          Arrays.stream(clustered.clustering())
              .map(expr -> expression(expr, input, target))
              .toArray(Expression[]::new));
    } else if (distribution instanceof OrderedDistribution ordered) {
      return Distributions.ordered(ordering(ordered.ordering(), input, target));
    }
    return distribution;
  }

  static SortOrder[] ordering(SortOrder[] ordering, Schema input, Schema target) {
    return Arrays.stream(ordering)
        .map(
            order ->
                Expressions.sort(
                    expression(order.expression(), input, target),
                    order.direction(),
                    order.nullOrdering()))
        .toArray(SortOrder[]::new);
  }

  private static Expression expression(Expression expr, Schema input, Schema target) {
    if (input == null) {
      return expr;
    }
    if (expr instanceof NamedReference ref) {
      Type type = target.asStruct();
      Types.NestedField field = null;
      for (String name : ref.fieldNames()) {
        if (!type.isStructType() || (field = type.asStructType().field(name)) == null) {
          return expr;
        }
        type = field.type();
      }
      Type source = field == null ? null : input.findType(field.fieldId());
      if (source != null
          && type.isPrimitiveType()
          && TypeUtil.isDateToTimestampPromotion(source, type.asPrimitiveType())) {
        return Expressions.apply("date_to_timestamp_ntz", expr);
      }
    } else if (expr instanceof Transform transform) {
      Expression[] args = transform.arguments();
      Expression[] promoted =
          Arrays.stream(args).map(arg -> expression(arg, input, target)).toArray(Expression[]::new);
      if (!Arrays.equals(args, promoted)) {
        if (transform.name().equals("identity")) {
          return promoted[0];
        }
        // Spark interprets "bucket" arguments as column references before binding functions.
        // Use the scalar alias when an argument includes a conversion expression.
        String name = transform.name().equals("bucket") ? "iceberg_bucket" : transform.name();
        return Expressions.apply(name, promoted);
      }
    }
    return expr;
  }

  static DataWriter<InternalRow> wrap(
      DataWriter<InternalRow> writer, StructType source, StructType target) {
    if (source.equals(target)) {
      return writer;
    }
    Function<Object, Object> convert = converter(source, target);
    return new DataWriter<>() {
      @Override
      public void write(InternalRow row) throws IOException {
        writer.write((InternalRow) convert.apply(row));
      }

      @Override
      public void write(InternalRow metadata, InternalRow row) throws IOException {
        writer.write(metadata, (InternalRow) convert.apply(row));
      }

      @Override
      public WriterCommitMessage commit() throws IOException {
        return writer.commit();
      }

      @Override
      public void abort() throws IOException {
        writer.abort();
      }

      @Override
      public void close() throws IOException {
        writer.close();
      }

      @Override
      public CustomTaskMetric[] currentMetricsValues() {
        return writer.currentMetricsValues();
      }
    };
  }

  private static Function<Object, Object> converter(DataType source, DataType target) {
    if (source.equals(target)) {
      return Function.identity();
    }
    Function<Object, Object> convert;
    if (DataTypes.DateType.equals(source) && DataTypes.TimestampNTZType.equals(target)) {
      convert = value -> DateToTimestampNtz.invoke((Integer) value);
    } else if (source instanceof StructType from && target instanceof StructType to) {
      StructField[] fields = from.fields();
      List<Function<Object, Object>> conversions = new ArrayList<>();
      for (int i = 0; i < fields.length; i++) {
        conversions.add(converter(fields[i].dataType(), to.fields()[i].dataType()));
      }
      convert =
          value -> {
            InternalRow row = (InternalRow) value;
            // Row lineage fields, when requested, are appended by the delegate writer.
            Object[] result = new Object[row.numFields()];
            for (int i = 0; i < result.length; i++) {
              result[i] =
                  conversions
                      .get(i)
                      .apply(row.isNullAt(i) ? null : row.get(i, fields[i].dataType()));
            }
            return new GenericInternalRow(result);
          };
    } else if (source instanceof ArrayType from && target instanceof ArrayType to) {
      Function<Object, Object> element = converter(from.elementType(), to.elementType());
      convert = value -> convertArray((ArrayData) value, from.elementType(), element);
    } else if (source instanceof MapType from && target instanceof MapType to) {
      Function<Object, Object> key = converter(from.keyType(), to.keyType());
      Function<Object, Object> mapValue = converter(from.valueType(), to.valueType());
      convert =
          value -> {
            MapData map = (MapData) value;
            return new ArrayBasedMapData(
                convertArray(map.keyArray(), from.keyType(), key),
                convertArray(map.valueArray(), from.valueType(), mapValue));
          };
    } else {
      throw new IllegalArgumentException("Cannot promote input type: " + source);
    }
    return value -> value == null ? null : convert.apply(value);
  }

  private static ArrayData convertArray(
      ArrayData array, DataType type, Function<Object, Object> convert) {
    Object[] result = new Object[array.numElements()];
    for (int i = 0; i < result.length; i++) {
      result[i] = convert.apply(array.isNullAt(i) ? null : array.get(i, type));
    }
    return new GenericArrayData(result);
  }
}
