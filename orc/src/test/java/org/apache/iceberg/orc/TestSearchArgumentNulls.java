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
package org.apache.iceberg.orc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.storage.ql.io.sarg.PredicateLeaf;
import org.apache.orc.storage.ql.io.sarg.SearchArgument;
import org.apache.orc.storage.ql.io.sarg.SearchArgument.TruthValue;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestSearchArgumentNulls {
  static Stream<Type.PrimitiveType> types() {
    return Stream.of(
        Types.IntegerType.get(),
        Types.DateType.get(),
        Types.TimestampType.withoutZone(),
        Types.TimestampNanoType.withoutZone());
  }

  @ParameterizedTest
  @MethodSource("types")
  void includesNullsForNegatedEqualityAndIn(Type.PrimitiveType type) {
    for (Expression predicate : List.of(Expressions.equal("v", 0L), Expressions.in("v", 0L, 1L))) {
      assertThat(evaluate(type, Expressions.not(predicate), true).isNeeded()).isTrue();
      assertThat(evaluate(type, predicate, true).isNeeded()).isFalse();
    }
  }

  @ParameterizedTest
  @MethodSource("types")
  void preservesNullOrderingForComparisons(Type.PrimitiveType type) {
    for (Expression predicate :
        List.of(Expressions.lessThan("v", 1L), Expressions.lessThanOrEqual("v", 1L))) {
      assertThat(evaluate(type, predicate, true).isNeeded()).isTrue();
      assertThat(evaluate(type, Expressions.not(predicate), true).isNeeded()).isFalse();
    }
    for (Expression predicate :
        List.of(Expressions.greaterThan("v", 1L), Expressions.greaterThanOrEqual("v", 1L))) {
      assertThat(evaluate(type, predicate, true).isNeeded()).isFalse();
      assertThat(evaluate(type, Expressions.not(predicate), true).isNeeded()).isTrue();
    }
  }

  @ParameterizedTest
  @MethodSource("types")
  void rewritesNegationOfConjunction(Type.PrimitiveType type) {
    Expression filter =
        Expressions.not(Expressions.and(Expressions.equal("v", 0L), Expressions.equal("id", 7)));
    assertThat(evaluate(type, filter, true).isNeeded()).isTrue();
    assertThat(evaluate(type, filter, false).isNeeded()).isTrue();
  }

  @ParameterizedTest
  @MethodSource("types")
  void rewritesNegationOfDisjunction(Type.PrimitiveType type) {
    Expression filter =
        Expressions.not(Expressions.or(Expressions.equal("v", 0L), Expressions.equal("id", 7)));
    assertThat(evaluate(type, filter, true).isNeeded()).isFalse();
    assertThat(evaluate(type, filter, false).isNeeded()).isTrue();
  }

  @ParameterizedTest
  @MethodSource("types")
  void prunesNonMatchingRangesWhenStatisticsExcludeNulls(Type.PrimitiveType type) {
    for (Expression predicate :
        List.of(Expressions.lessThan("v", 1L), Expressions.lessThanOrEqual("v", 1L))) {
      assertThat(evaluate(type, predicate, TruthValue.NO, TruthValue.NO, false).isNeeded())
          .isFalse();
      assertThat(
              evaluate(type, Expressions.not(predicate), TruthValue.NO, TruthValue.NO, false)
                  .isNeeded())
          .isTrue();
    }
    Expression negatedDisjunction =
        Expressions.not(Expressions.or(Expressions.equal("v", 0L), Expressions.equal("id", 7)));
    assertThat(evaluate(type, negatedDisjunction, TruthValue.NO, TruthValue.YES, false).isNeeded())
        .isFalse();
  }

  @ParameterizedTest
  @MethodSource("types")
  void retainsPossibleNullMatchesWhenNullStatisticsAreUnknown(Type.PrimitiveType type) {
    for (Expression predicate :
        List.of(Expressions.lessThan("v", 1L), Expressions.lessThanOrEqual("v", 1L))) {
      // The range cannot match, but absent null statistics do not prove the stripe has no nulls.
      assertThat(evaluate(type, predicate, TruthValue.YES_NO, TruthValue.NO, false).isNeeded())
          .isTrue();
    }
  }

  private static TruthValue evaluate(
      Type.PrimitiveType type, Expression filter, boolean idMatches) {
    return evaluate(type, filter, TruthValue.YES, TruthValue.NULL, idMatches);
  }

  private static TruthValue evaluate(
      Type.PrimitiveType type,
      Expression filter,
      TruthValue nulls,
      TruthValue comparison,
      boolean idMatches) {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "v", type),
            Types.NestedField.required(2, "id", Types.IntegerType.get()));
    SearchArgument sarg =
        ExpressionToSearchArgument.convert(
            Binder.bind(schema.asStruct(), filter, true), ORCSchemaUtil.convert(schema));
    TruthValue[] leaves =
        sarg.getLeaves().stream()
            .map(
                leaf -> {
                  if (leaf.getColumnName().equals("`v`")) {
                    return leaf.getOperator() == PredicateLeaf.Operator.IS_NULL
                        ? nulls
                        : comparison;
                  }
                  if (leaf.getOperator() == PredicateLeaf.Operator.IS_NULL) {
                    return TruthValue.NO;
                  }
                  return idMatches ? TruthValue.YES : TruthValue.NO;
                })
            .toArray(TruthValue[]::new);
    return sarg.evaluate(leaves);
  }
}
