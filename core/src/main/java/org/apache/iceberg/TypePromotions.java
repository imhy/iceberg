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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/** Table-specific validation and default conversion for field type promotions. */
class TypePromotions {
  private TypePromotions() {}

  /** Promotes a field after the caller has checked that the type change is allowed. */
  static Types.NestedField promote(
      TableMetadata base, String name, Types.NestedField field, Type.PrimitiveType newType) {
    Types.NestedField.Builder builder = Types.NestedField.from(field).ofType(newType);
    if (TypeUtil.isDateToTimestampPromotion(field.type(), newType)) {
      Preconditions.checkNotNull(
          base, "Cannot validate date promotion without base table metadata");
      validateDatePromotion(base.specs(), base.sortOrders(), field.fieldId(), name);
    }

    return builder.build();
  }

  static void validateDatePromotions(
      int formatVersion,
      Schema previous,
      Schema updated,
      Iterable<PartitionSpec> specs,
      Iterable<SortOrder> orders) {
    if (previous == null) {
      return;
    }

    for (int fieldId : previous.idToName().keySet()) {
      Type target = updated.findType(fieldId);
      if (target != null
          && target.isPrimitiveType()
          && TypeUtil.isDateToTimestampPromotion(
              previous.findType(fieldId), target.asPrimitiveType())) {
        Preconditions.checkArgument(
            TypeUtil.isPromotionAllowed(
                formatVersion, previous.findType(fieldId), target.asPrimitiveType()),
            "Cannot promote date column %s in format version %s: requires v3 or later",
            updated.findColumnName(fieldId),
            formatVersion);
        validateDatePromotion(specs, orders, fieldId, updated.findColumnName(fieldId));
      }
    }
  }

  private static void validateDatePromotion(
      Iterable<PartitionSpec> specs, Iterable<SortOrder> orders, int fieldId, String name) {
    for (PartitionSpec spec : specs) {
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

    for (SortOrder order : orders) {
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
}
