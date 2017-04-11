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

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

class CsvAttributes(val delimiter: Char = ',',
                    val quotechar: Char = '\"',
                    val hasHeader: Boolean = false)
    extends Serializable {
  def isExcelCSV: Boolean = quotechar == '"'

  def fromSchema(schema: SmvSchema): CsvAttributes = this

  override def equals(other: Any): Boolean = other match {
    case attr: CsvAttributes =>
      (attr.delimiter == delimiter) &&
        (attr.quotechar == quotechar) && (attr.hasHeader == hasHeader)
    case _ => false
  }
}

private case object InferFromSchemaCsvAttributes extends CsvAttributes {
  override def fromSchema(schema: SmvSchema): CsvAttributes = schema.extractCsvAttributes()
}

object CsvAttributes {
  lazy val defaultCsvAttributes: CsvAttributes = new CsvAttributes()

  // common CsvAttributes combos to be imported explicitly
  lazy val defaultTsv: CsvAttributes           = new CsvAttributes(delimiter = '\t')
  lazy val defaultCsvWithHeader: CsvAttributes = new CsvAttributes(hasHeader = true)
  lazy val defaultTsvWithHeader: CsvAttributes =
    new CsvAttributes(delimiter = '\t', hasHeader = true)
  implicit val inferFromSchema: CsvAttributes = InferFromSchemaCsvAttributes

  def dropHeader[T](rdd: RDD[T])(implicit tt: ClassTag[T]) = {
    val dropFunc = new DropRDDFunctions(rdd)
    dropFunc.drop(1)
  }
}
