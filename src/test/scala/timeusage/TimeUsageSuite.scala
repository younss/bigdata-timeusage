package timeusage

import org.apache.avro.generic.GenericData
import org.apache.spark.sql.{Column, ColumnName, DataFrame, Row}
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {
  import TimeUsage._

  import spark.implicits._

  test("dfSchema method returns a StructType describing the schema of the CSV file first Field is String, others Are Double") {
    val firstLine = List("A", "B")
    val result = dfSchema(firstLine)
    assert(result.contains(StructField("A", StringType, false)))
    assert(result.contains(StructField("B", DoubleType, false)))

  }

  test("return An RDD Row compatible with the schema produced by `dfSchema`") {
    val secondLine = List("A", "0.1")
    val rowResult = row(secondLine)
    assert(rowResult.get(1).equals(0.1d))
    assert(rowResult.get(0).equals("A"))
  }

  test("classifies the given list of column names into three Column groups (primary needs, work or other). This method should return a triplet containing the 'primary needs' columns list, the 'work' columns list and the 'other' columns list.") {
    /*
        "primary needs” activities (sleeping, eating, etc.)
              are reported in columns starting with "t01”, "t03”, “t11”, “t1801” and “t1803” ;
        working activities
              are reported in columns starting with “t05” and “t1805” ;
        other activities (leisure) are reported in columns starting with
            “t02”, “t04”, “t06”, “t07”, “t08”, “t09”, “t10”, “t12”, “t13”, “t14”, “t15”, “t16”
                and “t18” (only those which are not part of the previous groups).

     */
    val fistLine = List("t011", "t03", "t11", "t1801", "t1803", "t05", "t1805", "t02", "t04", "t06", "t07", "t08", "t09", "t10", "t12", "t13", "t14", "t15", "t16", "t1890", "t1891")

    val result = classifiedColumns(fistLine)
    assert(result._2.sameElements(List($"t1805", $"t05")))

  }


  test("'timeUsageSummary' return a projection of the initial DataFrame such that all columns containing hours spent on primary needs are summed together in a single column (and same for work and leisure). The “teage” column is also  projected to three values: young, active, elder"){

    val primaryNeedsColumns: List[Column] = List(new Column("primaryNeeds1"),new Column("primaryNeeds2"))
    val workColumns: List[Column]= List(new Column("work1"),new Column("work2"))
    val otherColumns: List[Column]= List(new Column("other1"),new Column("other2"))
    val schema = List(
      StructField("telfs", IntegerType, false),
      StructField("tesex", IntegerType, false),
      StructField("teage",IntegerType,false),
      StructField("primaryNeeds1",IntegerType,false),
      StructField("primaryNeeds2",IntegerType,false),
      StructField("work1",IntegerType,false),
      StructField("work2",IntegerType,false),
      StructField("other1",IntegerType,false),
      StructField("other2",IntegerType,false)
    )
    val data = Seq(
      Row(3, 1,26,60,120,60,120,60,120),
      Row(3, 2,26,60,120,60,120,60,120),
      Row(5, 1,26,60,120,60,120,60,120)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val targetSchema = List(
      StructField("working", StringType, false),
      StructField("sex", StringType, false),
      StructField("age",StringType,false),
      StructField("primaryNeeds",DoubleType,false),
      StructField("work",DoubleType,false),
      StructField("other",DoubleType,false)
    )
    val targetData = Seq(
      Row("not working", "male","active", 3.0,3.0,3.0),
      Row("not working", "female","active", 3.0,3.0,3.0)
    )

    val targetDf = spark.createDataFrame(
      spark.sparkContext.parallelize(targetData),
      StructType(targetSchema)
    )

    // Result
    //  +-----------+------+------+------------+----+-----+
    //  |    working|   sex|   age|primaryNeeds|work|other|
    //  +-----------+------+------+------------+----+-----+
    //  |not working|  male|active|         3.0| 3.0|  3.0|
    //  |not working|female|active|         3.0| 3.0|  3.0|
    //  +-----------+------+------+------------+----+-----+


    val result  =  timeUsageSummary(primaryNeedsColumns,workColumns,otherColumns,df)
    assert(result.schema.map(sf=> sf.name).sameElements(targetDf.schema.map(sf=> sf.name))," schema are different that expected")
    assert(result.collect().sameElements(targetDf.collect())," result are different that expected")

  }


}
