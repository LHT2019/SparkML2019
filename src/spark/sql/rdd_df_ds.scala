package spark.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

/**
  * Created by root on 2019/2/19.
  */
case class People(name:String, age:Int)
object rdd_df {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark: SparkSession = SparkSession.builder().appName("sql_t01").master("local[*]").getOrCreate()
    import spark.implicits._
    val personDF = spark.sparkContext.textFile("src/spark/data/people.txt").map(_.split(",")).map(fileds=>{
      Person(fileds(0), fileds(1).toInt)
    }).toDF()

    personDF.createOrReplaceTempView("people")
    spark.sql("select * from people").show()

    personDF.map(person=> "name : " + person(0) ).show()

    // 保存数据
    personDF.select("name","age").write.format("csv").save("src/spark/data/people_save_csv")
    personDF.select("name","age").write.format("parquet").save("src/spark/data/people_save_parquet")
    // 读取数据
    spark.sql("select * from csv.`src/spark/data/people_save_csv`").show(100)
    spark.sql("select * from parquet.`src/spark/data/people_save_parquet`").show(100)

 }
}
