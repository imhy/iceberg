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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/** An owned input snapshot with conversions only for the fields whose types changed. */
final class PromotedInternalRow extends InternalRow {
  private final InternalRow row;
  private final StructField[] targetFields;
  private final int[] positions;
  private final Object[] values;

  PromotedInternalRow(
      InternalRow row, StructField[] targetFields, int[] positions, Object[] values) {
    this.row = row;
    this.targetFields = targetFields;
    this.positions = positions;
    this.values = values;
  }

  @Override
  public int numFields() {
    return row.numFields();
  }

  @Override
  public boolean isNullAt(int pos) {
    return row.isNullAt(pos);
  }

  @Override
  public Object get(int pos, DataType type) {
    return positions[pos] < 0 ? row.get(pos, type) : values[positions[pos]];
  }

  @Override
  public long getLong(int pos) {
    return positions[pos] < 0 ? row.getLong(pos) : (long) values[positions[pos]];
  }

  @Override
  public InternalRow getStruct(int pos, int fields) {
    return positions[pos] < 0 ? row.getStruct(pos, fields) : (InternalRow) values[positions[pos]];
  }

  @Override
  public ArrayData getArray(int pos) {
    return positions[pos] < 0 ? row.getArray(pos) : (ArrayData) values[positions[pos]];
  }

  @Override
  public MapData getMap(int pos) {
    return positions[pos] < 0 ? row.getMap(pos) : (MapData) values[positions[pos]];
  }

  @Override
  public void setNullAt(int pos) {
    throw new UnsupportedOperationException("PromotedInternalRow is read-only");
  }

  @Override
  public void update(int pos, Object value) {
    throw new UnsupportedOperationException("PromotedInternalRow is read-only");
  }

  @Override
  public InternalRow copy() {
    Object[] copiedValues = new Object[numFields()];
    for (int i = 0; i < copiedValues.length; i++) {
      copiedValues[i] = InternalRow.copyValue(get(i, targetFields[i].dataType()));
    }
    return new GenericInternalRow(copiedValues);
  }

  @Override
  public boolean getBoolean(int pos) {
    return row.getBoolean(pos);
  }

  @Override
  public byte getByte(int pos) {
    return row.getByte(pos);
  }

  @Override
  public short getShort(int pos) {
    return row.getShort(pos);
  }

  @Override
  public int getInt(int pos) {
    return row.getInt(pos);
  }

  @Override
  public float getFloat(int pos) {
    return row.getFloat(pos);
  }

  @Override
  public double getDouble(int pos) {
    return row.getDouble(pos);
  }

  @Override
  public Decimal getDecimal(int pos, int precision, int scale) {
    return row.getDecimal(pos, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int pos) {
    return row.getUTF8String(pos);
  }

  @Override
  public byte[] getBinary(int pos) {
    return row.getBinary(pos);
  }

  @Override
  public CalendarInterval getInterval(int pos) {
    return row.getInterval(pos);
  }
}
