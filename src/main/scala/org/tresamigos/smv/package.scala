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

package org.tresamigos

import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

package object smv {
  implicit def makeSRHelper(sc: SQLContext) = new SqlContextHelper(sc)
  implicit def makeSchemaRDDHelper(srdd: SchemaRDD) = new SchemaRDDHelper(srdd)

  implicit def makeRDDHelper(rdd: RDD[String]) = new SingleStrRDDHelper(rdd)
  implicit def makeKeyStrRDDHelper(rdd: RDD[(String, String)]) = new KeyStrRDDHelper(rdd)
}