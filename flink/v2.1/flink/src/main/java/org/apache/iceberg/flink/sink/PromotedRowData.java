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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.variant.Variant;

/** Reads unchanged fields directly from an owned row snapshot and converts changed fields. */
final class PromotedRowData implements RowData {
  private final RowData row;
  private final int[] positions;
  private final Object[] values;

  PromotedRowData(RowData row, int[] positions, Object[] values) {
    this.row = row;
    this.positions = positions;
    this.values = values;
  }

  @Override
  public int getArity() {
    return row.getArity();
  }

  @Override
  public RowKind getRowKind() {
    return row.getRowKind();
  }

  @Override
  public void setRowKind(RowKind kind) {
    row.setRowKind(kind);
  }

  @Override
  public boolean isNullAt(int pos) {
    return row.isNullAt(pos);
  }

  @Override
  public TimestampData getTimestamp(int pos, int precision) {
    return positions[pos] < 0
        ? row.getTimestamp(pos, precision)
        : (TimestampData) values[positions[pos]];
  }

  @Override
  public RowData getRow(int pos, int fields) {
    return positions[pos] < 0 ? row.getRow(pos, fields) : (RowData) values[positions[pos]];
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
  public long getLong(int pos) {
    return row.getLong(pos);
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
  public DecimalData getDecimal(int pos, int precision, int scale) {
    return row.getDecimal(pos, precision, scale);
  }

  @Override
  public StringData getString(int pos) {
    return row.getString(pos);
  }

  @Override
  public byte[] getBinary(int pos) {
    return row.getBinary(pos);
  }

  @Override
  public <T> RawValueData<T> getRawValue(int pos) {
    return row.getRawValue(pos);
  }

  @Override
  public Variant getVariant(int pos) {
    return row.getVariant(pos);
  }
}
