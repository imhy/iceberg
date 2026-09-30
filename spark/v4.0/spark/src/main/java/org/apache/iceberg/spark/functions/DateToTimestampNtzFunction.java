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
package org.apache.iceberg.spark.functions;

import org.apache.iceberg.util.DateTimeUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/** Converts a date to a timestamp without time zone at local midnight. */
public class DateToTimestampNtzFunction extends UnaryUnboundFunction {
  @Override
  protected BoundFunction doBind(DataType type) {
    if (!DataTypes.DateType.equals(type)) {
      throw new UnsupportedOperationException("Expected date: " + type.catalogString());
    }
    return new DateToTimestampNtz();
  }

  @Override
  public String name() {
    return "date_to_timestamp_ntz";
  }

  @Override
  public String description() {
    return "date_to_timestamp_ntz(date) - Convert a date to local midnight without a time zone";
  }

  public static class DateToTimestampNtz extends BaseScalarFunction<Long> {
    // Spark invokes this method from generated code for write distribution and ordering.
    public static long invoke(int days) {
      return DateTimeUtil.microsFromDays(days);
    }

    @Override
    public String name() {
      return "date_to_timestamp_ntz";
    }

    @Override
    public String canonicalName() {
      return "iceberg.date_to_timestamp_ntz(date)";
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.DateType};
    }

    @Override
    public DataType resultType() {
      return DataTypes.TimestampNTZType;
    }

    @Override
    public Long produceResult(InternalRow input) {
      return input.isNullAt(0) ? null : invoke(input.getInt(0));
    }
  }
}
