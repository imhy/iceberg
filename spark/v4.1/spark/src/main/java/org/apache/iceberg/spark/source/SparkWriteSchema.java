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

import java.util.List;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/** Preserves Iceberg field attributes when a validated Spark write promotes DATE values. */
final class SparkWriteSchema extends TypeUtil.SchemaVisitor<Type> {
  private final Schema target;

  private SparkWriteSchema(Schema target) {
    this.target = target;
  }

  static Schema promote(Schema input, Schema target) {
    Schema defaultSource =
        input.findField(MetadataColumns.ROW_ID.fieldId()) == null
            ? target
            : MetadataColumns.schemaWithRowLineage(target);
    Types.StructType struct =
        TypeUtil.visit(input, new SparkWriteSchema(defaultSource)).asStructType();
    Schema promoted =
        new Schema(
            input.schemaId(), struct.fields(), input.getAliases(), input.identifierFieldIds());
    return TypeUtil.reassignDefaults(promoted, defaultSource);
  }

  @Override
  public Type schema(Schema schema, Type struct) {
    return struct;
  }

  @Override
  public Type struct(Types.StructType struct, List<Type> types) {
    List<Types.NestedField> fields = Lists.newArrayListWithExpectedSize(types.size());
    for (int i = 0; i < types.size(); i++) {
      Types.NestedField field = struct.fields().get(i);
      Type type = types.get(i);
      Types.NestedField.Builder builder = Types.NestedField.from(field).ofType(type);
      if (isDatePromotion(field.type(), type)) {
        builder
            .withInitialDefault(promoteDefault(field.initialDefaultLiteral(), type))
            .withWriteDefault(promoteDefault(field.writeDefaultLiteral(), type));
      }
      fields.add(builder.build());
    }
    return Types.StructType.of(fields);
  }

  @Override
  public Type field(Types.NestedField field, Type result) {
    return promoteType(field.type(), result, target.findType(field.fieldId()));
  }

  @Override
  public Type list(Types.ListType list, Type element) {
    Type promoted = promoteType(list.elementType(), element, target.findType(list.elementId()));
    return list.isElementOptional()
        ? Types.ListType.ofOptional(list.elementId(), promoted)
        : Types.ListType.ofRequired(list.elementId(), promoted);
  }

  @Override
  public Type map(Types.MapType map, Type key, Type value) {
    Type promotedKey = promoteType(map.keyType(), key, target.findType(map.keyId()));
    Type promotedValue = promoteType(map.valueType(), value, target.findType(map.valueId()));
    return map.isValueOptional()
        ? Types.MapType.ofOptional(map.keyId(), map.valueId(), promotedKey, promotedValue)
        : Types.MapType.ofRequired(map.keyId(), map.valueId(), promotedKey, promotedValue);
  }

  @Override
  public Type primitive(Type.PrimitiveType primitive) {
    return primitive;
  }

  @Override
  public Type variant(Types.VariantType variant) {
    return variant;
  }

  private static Type promoteType(Type input, Type result, Type target) {
    return isDatePromotion(input, target) ? target : result;
  }

  private static boolean isDatePromotion(Type input, Type target) {
    return target != null
        && target.isPrimitiveType()
        && TypeUtil.isDateToTimestampPromotion(input, target.asPrimitiveType());
  }

  private static Literal<?> promoteDefault(Literal<?> value, Type target) {
    return value == null ? null : value.to(target);
  }
}
