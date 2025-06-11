/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*** spark-rapids-shim-json-lines
 {"spark": "320"}
 {"spark": "321"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.hive.bytedance

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{RowNumberLike, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class OptimizePlanRules() extends Rule[LogicalPlan] with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
    case q: LogicalPlan =>
      q.transformExpressions {
        case we@WindowExpression(AggregateExpression(r: RowNumberLike, _, _, _, _), _) =>
          we.copy(windowFunction = r)
      }
  }
}
