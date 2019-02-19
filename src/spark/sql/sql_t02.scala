package spark.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
  * Created by root on 2019/2/19.
  */

case class Person(name:String, age:Long)

object sql_t02 {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark: SparkSession = SparkSession.builder().appName("sql_t01").master("local[*]").getOrCreate()

    import spark.implicits._
    val stu: DataFrame = spark.read.format("csv").option("delimiter", ",").option("header","true").load("src/spark/data/sql_t02.txt")
    stu.printSchema()
    stu.select("name").show()
    stu.select($"name", $"age",$"age" + 1).show()
    stu.filter($"age" > 21).show()
    stu.groupBy("age").count().show()
    stu.map(s=>"name : " + s(0) ).show()

    // DataSet
    val caseClassDS: Dataset[Person] = Seq(Person("Andy", 23), Person("ShiShi.Liu",22)).toDS()
    caseClassDS.show()
    caseClassDS.select($"name", $"age", $"age" + 1).show()

    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_+1).show()

//    val peopleDS: Dataset[Person] = spark.read.csv("src/spark/data/people.txt").as[Person]
//    val peopleDS: Dataset[Person] = spark.read.format("csv").option("delimiter",",").option("header", false).load("src/spark/data/people.txt").as[Person]

    val peopleDF: DataFrame = spark.read.csv("src/spark/data/people.txt")
//    val peopleRDD: RDD[String] = spark.sparkContext.textFile("src/spark/data/people.txt")

    peopleDF.show()

//    val sturdd: RDD[Row] = stu.rdd
//    val studs: Dataset[Row] = spark.createDataset(sturdd)
//    studs.createTempView("stu")
//    spark.sql("select * from stu").show()
    // studs.select($"name", $"age", $"score", $"score" + 1).show()
  }
}
