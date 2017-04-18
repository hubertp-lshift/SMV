/*
 * This file is licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tresamigos.smv

/**
 * Instead of using String for join type, always use the link here.
 *
 * If there are typos on the join type, using the link in client code will cause
 * compile time failure, which using string itself will cause run-time failure.
 *
 * Spark(as of 1.4)'s join type is a String.  Could use enum or case objects here,
 * but there are clients using the String api, so will push that change till later.
 *
 */
sealed abstract class SmvJoinType(val name: String)

object SmvJoinType {
  case object Inner      extends SmvJoinType("inner")
  case object Outer      extends SmvJoinType("outer")
  case object LeftOuter  extends SmvJoinType("leftouter")
  case object RightOuter extends SmvJoinType("rightouter")
  case object Semi       extends SmvJoinType("leftsemi")

  def apply(x: String): SmvJoinType = x match {
    case Inner.name      => Inner
    case Outer.name      => Outer
    case LeftOuter.name  => LeftOuter
    case RightOuter.name => RightOuter
    case Semi.name       => Semi
    case _               => throw new SmvRuntimeException(s"Invalid Join Type: $x")
  }
}
