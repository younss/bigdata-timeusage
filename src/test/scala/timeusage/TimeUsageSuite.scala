package timeusage

import org.apache.avro.generic.GenericData
import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {
  import TimeUsage._

    test("dfSchema method returns a StructType describing the schema of the CSV file first Field is String, others Are Double") {
    val firstLine = List("A","B")
      val result = dfSchema(firstLine)
      assert(result.contains(StructField("A",StringType,false)))
      assert(result.contains(StructField("B",DoubleType,false)))

  }

  test ("return An RDD Row compatible with the schema produced by `dfSchema`"){
    val secondLine = List("A","0.1")
    val rowResult = row(secondLine)
    println(rowResult)
    assert(rowResult.get(1).equals(0.1d))
    assert(rowResult.get(0).equals("A"))
  }

}
