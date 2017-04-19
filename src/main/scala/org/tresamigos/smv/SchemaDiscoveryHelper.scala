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
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.tresamigos.smv.util.StringConversion._

import scala.collection.mutable
import scala.util.Try

class SchemaDiscoveryHelper(sqlContext: SQLContext) {
  import SchemaDiscoveryHelper._

  /**
   * Extract the column names from the csv header if it has one. In case of multi-line header the last header line is
   * considered the one that holds the column names. If there is no header the columns will be named f1, f2, f3 ...
   * @param strRDD  holds the content of the csv file including the header if it has one.
   * @param ca the csv file attributes
   * @return the discovered schema
   */
  private def getColumnNames(strRDD: RDD[String])(implicit ca: CsvAttributes): Array[String] = {
    val parser = new CSVParserWrapper

    if (ca.hasHeader) {
      val columnNamesRowStr = strRDD.first()
      var columnNames       = parser.parseLine(columnNamesRowStr)
      columnNames = columnNames.map(_.trim).map(SchemaEntry.valueToColumnName(_))

      columnNames
    } else {
      val firstRowStr     = strRDD.first()
      val firstRowValues  = parser.parseLine(firstRowStr)
      val numberOfColumns = firstRowValues.length

      val columnNames = for (i <- 1 to numberOfColumns) yield "f" + i

      columnNames.toArray
    }
  }

  private def canConvertToyyyyMMddDate(str: String): Boolean =
    str match {
      case dateR(_, month0, day0) =>
        val month = month0.toInt
        val day   = day0.toInt

        month >= 1 && month <= 12 && day >= 1 && day <= 31
      case _ =>
        false
    }

  /**
   * Discover the type of a given column based on it value. Also perform type promotion to
   * accommodate all the possible values.
   * TODO: should consider using Decimal for large integer/float values (more than what can fit in long/double)
   */
  private def getTypeFormat(curTypeFormat: Option[TypeFormat], valueStr: String): TypeFormat = {
    if (valueStr.isEmpty) curTypeFormat.getOrElse(StringTypeFormat())
    else
      curTypeFormat match {

        //Handling the initial case where the current column schema entry is not set yet
        case None if canConvertToInt(valueStr) && canConvertToyyyyMMddDate(valueStr) =>
          DateTypeFormat("yyyyMMdd")
        case None if canConvertToInt(valueStr)                 => IntegerTypeFormat()
        case None if canConvertToLong(valueStr)                => LongTypeFormat()
        case None if canConvertToFloat(valueStr)               => FloatTypeFormat()
        case None if canConvertToDouble(valueStr)              => DoubleTypeFormat()
        case None if canConvertToBoolean(valueStr)             => BooleanTypeFormat()
        case None if canConvertToDate(valueStr, "dd/MM/yyyy")  => DateTypeFormat("dd/MM/yyyy")
        case None if canConvertToDate(valueStr, "dd-MM-yyyy")  => DateTypeFormat("dd-MM-yyyy")
        case None if canConvertToDate(valueStr, "dd-MMM-yyyy") => DateTypeFormat("dd-MMM-yyyy")
        case None if canConvertToDate(valueStr, "ddMMMyyyy")   => DateTypeFormat("ddMMMyyyy")
        case None if canConvertToDate(valueStr, "yyyy-MM-dd")  => DateTypeFormat("yyyy-MM-dd")
        case None                                              => StringTypeFormat()

        // Handling Integer type and its possible promotions
        case Some(fmt @ IntegerTypeFormat(_)) if canConvertToInt(valueStr) => fmt
        case Some(IntegerTypeFormat(_)) if canConvertToLong(valueStr)      => LongTypeFormat()
        case Some(IntegerTypeFormat(_)) if canConvertToFloat(valueStr)     => FloatTypeFormat()
        case Some(IntegerTypeFormat(_)) if canConvertToDouble(valueStr)    => DoubleTypeFormat()
        case Some(IntegerTypeFormat(_))                                    => StringTypeFormat()

        // Handling Long type and its possible promotions
        case Some(fmt @ LongTypeFormat(_)) if canConvertToLong(valueStr) => fmt
        case Some(LongTypeFormat(_)) if canConvertToDouble(valueStr)     => DoubleTypeFormat()
        case Some(LongTypeFormat(_))                                     => StringTypeFormat()

        // Handling Float type and its possible promotions
        case Some(fmt @ FloatTypeFormat(_)) if canConvertToFloat(valueStr) => fmt
        case Some(FloatTypeFormat(_)) if canConvertToDouble(valueStr)      => DoubleTypeFormat()
        case Some(FloatTypeFormat(_))                                      => StringTypeFormat()

        // Handling Double type and its possible promotions
        case Some(fmt @ DoubleTypeFormat(_)) if canConvertToDouble(valueStr) => fmt
        case Some(DoubleTypeFormat(_))                                       => StringTypeFormat()

        // Handling Boolean type and its possible promotions
        case Some(fmt @ BooleanTypeFormat(_)) if canConvertToBoolean(valueStr) => fmt
        case Some(BooleanTypeFormat(_))                                        => StringTypeFormat()

        //TODO: Need to find a better way to match dates to avoid repetition.

        //The date format should not change,) if it did then we will treat the column as String
        case Some(fmt @ DateTypeFormat("yyyyMMdd")) if canConvertToyyyyMMddDate(valueStr) => fmt
        case Some(DateTypeFormat("yyyyMMdd")) if canConvertToInt(valueStr)                => IntegerTypeFormat()
        case Some(DateTypeFormat("yyyyMMdd")) if canConvertToLong(valueStr)               => LongTypeFormat()
        case Some(DateTypeFormat("yyyyMMdd")) if canConvertToFloat(valueStr)              => FloatTypeFormat()
        case Some(DateTypeFormat("yyyyMMdd")) if canConvertToDouble(valueStr)             => DoubleTypeFormat()
        case Some(DateTypeFormat("yyyyMMdd"))                                             => StringTypeFormat()

        case Some(fmt @ DateTypeFormat("dd/MM/yyyy"))
            if canConvertToDate(valueStr, "dd/MM/yyyy") =>
          fmt
        case Some(DateTypeFormat("dd/MM/yyyy")) => StringTypeFormat()

        case Some(fmt @ DateTypeFormat("dd-MM-yyyy"))
            if canConvertToDate(valueStr, "dd-MM-yyyy") =>
          fmt
        case Some(DateTypeFormat("dd-MM-yyyy")) => StringTypeFormat()

        case Some(fmt @ DateTypeFormat("dd-MMM-yyyy"))
            if canConvertToDate(valueStr, "dd-MMM-yyyy") =>
          fmt
        case Some(DateTypeFormat("dd-MMM-yyyy")) => StringTypeFormat()

        case Some(fmt @ DateTypeFormat("ddMMMyyyy")) if canConvertToDate(valueStr, "ddMMMyyyy") =>
          fmt
        case Some(DateTypeFormat("ddMMMyyyy")) => StringTypeFormat()

        case Some(fmt @ DateTypeFormat("yyyy-MM-dd"))
            if canConvertToDate(valueStr, "yyyy-MM-dd") =>
          fmt
        case Some(DateTypeFormat("yyyy-MM-dd")) => StringTypeFormat()

        case Some(fmt @ StringTypeFormat(_, _)) => fmt

        case Some(_) => StringTypeFormat()
      }
  }

  /**
   * Discover the schema associated with a csv file that was converted to RDD[String]. If the csv file have a header,
   * the column names will be what the header specify, in case of multi-line header, the last line in the header is
   * considered the one that specify the column names. If there is no header the column names will be f1, f2, ... fn.
   * @param strRDD the content of the csv file read as RDD[String]. This should include the header if the csv file has one.
   * @param numLines the number of rows to process to discover the type of the columns
   * @param ca  the csv file attributes
   */
  private[smv] def discoverSchema(strRDD: RDD[String], numLines: Int)(
      implicit ca: CsvAttributes): SmvSchema = {
    val parser = new CSVParser(ca.delimiter)

    val columns = getColumnNames(strRDD)

    val noHeadRDD = if (ca.hasHeader) CsvAttributes.dropHeader(strRDD) else strRDD

    var typeFmts = new mutable.HashMap[String, TypeFormat]()

    //TODO: What if the numLines is so big that rowsToParse will not fit in memory
    // An alternative is to use the mapPartitionWithIndex
    val rowsToParse = noHeadRDD.take(numLines)

    val columnsWithIndex = columns.zipWithIndex

    var validCount = false
    for (rowStr <- rowsToParse) {
      val rowValues = Try { parser.parseLine(rowStr) }.getOrElse(Array[String]())
      if (rowValues.length == columnsWithIndex.length) {
        validCount = true
        columns.zipWithIndex.foreach {
          case (column, index) =>
            val colVal = rowValues(index)
            if (colVal.nonEmpty) typeFmts(column) = getTypeFormat(typeFmts.get(column), colVal)
        }
      }
    }

    // handle case where we were not able to parse a single valid data line.
    if (!validCount)
      throw new IllegalStateException("Unable to find a single valid data line")

    //Now we should set the null schema entries to the Default StringSchemaEntry. This should be the case when the first
    //numLines values for a given column happen to be missing.
    // Note: SmvSchema relies on the order of formatters, so we cannot just map typeFmts Map
    // without taking into account columns order
    val orderedFmts = columns.map { column =>
      if (!typeFmts.contains(column)) (column, StringTypeFormat()) else (column, typeFmts(column))
    }

    new SmvSchema(orderedFmts.map { case (n, t) => SchemaEntry(n, t) }, Map.empty) addCsvAttributes (ca)
  }

  /**
   * Discover schema from file
   * @param dataPath the path to the csv file (a schema file should be a sister file of the csv)
   * @param numLines the number of rows to process in order to discover the column types
   * @param ca the csv file attributes
   */
  def discoverSchemaFromFile(dataPath: String, numLines: Int = 1000)(
      implicit ca: CsvAttributes): SmvSchema = {
    val strRDD = sqlContext.sparkContext.textFile(dataPath)
    discoverSchema(strRDD, numLines)
  }
}

object SchemaDiscoveryHelper {
  private val dateR = """(\d\d\d\d)(\d\d)(\d\d)""".r
}
