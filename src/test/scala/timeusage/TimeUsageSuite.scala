package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {
  import TimeUsage._

    test("dfSchema method returns a StructType describing the schema of the CSV file") {
    val firstLine = List("A","B","C","D")
      val result = dfSchema(firstLine)
      assert(result.head.dataType.eq(StringType))
      assert(result.tail.head.dataType.eq(DoubleType))

  }
}
