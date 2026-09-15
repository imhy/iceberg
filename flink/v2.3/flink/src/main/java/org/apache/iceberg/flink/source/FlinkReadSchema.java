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
package org.apache.iceberg.flink.source;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Restores table defaults lost when a read projection is converted through Flink types. */
final class FlinkReadSchema {
  private FlinkReadSchema() {}

  static Schema withDefaults(Schema tableSchema, Schema projection) {
    Types.StructType struct = copyDefaults(tableSchema, projection.asStruct()).asStructType();
    return new Schema(projection.schemaId(), struct.fields(), projection.identifierFieldIds());
  }

  private static Type copyDefaults(Schema tableSchema, Type type) {
    return switch (type.typeId()) {
      case STRUCT -> {
        List<Types.NestedField> fields = Lists.newArrayList();
        for (Types.NestedField field : type.asStructType().fields()) {
          Types.NestedField tableField = tableSchema.findField(field.fieldId());
          Preconditions.checkNotNull(
              tableField, "Cannot find projected field ID: %s", field.fieldId());
          fields.add(
              Types.NestedField.from(field)
                  .ofType(copyDefaults(tableSchema, field.type()))
                  .withInitialDefault(tableField.initialDefaultLiteral())
                  .withWriteDefault(tableField.writeDefaultLiteral())
                  .build());
        }
        yield Types.StructType.of(fields);
      }
      case LIST -> {
        Types.ListType list = type.asListType();
        Type element = copyDefaults(tableSchema, list.elementType());
        yield list.isElementOptional()
            ? Types.ListType.ofOptional(list.elementId(), element)
            : Types.ListType.ofRequired(list.elementId(), element);
      }
      case MAP -> {
        Types.MapType map = type.asMapType();
        Type key = copyDefaults(tableSchema, map.keyType());
        Type value = copyDefaults(tableSchema, map.valueType());
        yield map.isValueOptional()
            ? Types.MapType.ofOptional(map.keyId(), map.valueId(), key, value)
            : Types.MapType.ofRequired(map.keyId(), map.valueId(), key, value);
      }
      default -> type;
    };
  }
}
