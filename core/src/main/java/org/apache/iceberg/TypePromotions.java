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
package org.apache.iceberg;

import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

/** Table-specific validation and default conversion for field type promotions. */
class TypePromotions {
  private TypePromotions() {}

  /** Promotes a field after the caller has checked that the type change is allowed. */
  static Types.NestedField promote(
      TableMetadata base, String name, Types.NestedField field, Type.PrimitiveType newType) {
    Types.NestedField.Builder builder = Types.NestedField.from(field).ofType(newType);
    if (TypeUtil.isDateToTimestampPromotion(field.type(), newType)) {
      validateDatePromotion(base, field.fieldId(), name);
      builder
          .withInitialDefault(promoteDateDefault(field.initialDefaultLiteral(), newType))
          .withWriteDefault(promoteDateDefault(field.writeDefaultLiteral(), newType));
    }

    return builder.build();
  }

  private static void validateDatePromotion(TableMetadata base, int fieldId, String name) {
    for (PartitionSpec spec : base.specs()) {
      for (PartitionField field : spec.fields()) {
        if (field.sourceId() == fieldId) {
          Preconditions.checkArgument(
              preservesDatePartitionValue(field.transform().toString()),
              "Cannot promote date column %s: partition spec %s uses incompatible transform %s",
              name,
              spec.specId(),
              field.transform());
        }
      }
    }

    for (SortOrder order : base.sortOrders()) {
      for (SortField field : order.fields()) {
        if (field.sourceId() == fieldId) {
          Preconditions.checkArgument(
              field.transform().isIdentity()
                  || preservesDatePartitionValue(field.transform().toString()),
              "Cannot promote date column %s: sort order %s uses incompatible transform %s",
              name,
              order.orderId(),
              field.transform());
        }
      }
    }
  }

  private static boolean preservesDatePartitionValue(String transform) {
    return switch (transform) {
      case "year", "month", "day", "void" -> true;
      default -> false;
    };
  }

  private static Literal<?> promoteDateDefault(Literal<?> value, Type.PrimitiveType target) {
    if (value == null) {
      return null;
    }

    long micros = DateTimeUtil.microsFromDays((Integer) value.value());
    return Literal.of(micros).to(target);
  }
}
