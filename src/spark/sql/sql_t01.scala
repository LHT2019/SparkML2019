package spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.Level
import org.apache.log4j.Logger

/**
  * Created by root on 2019/2/18.
  */
object sql_t01 {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark: SparkSession = SparkSession.builder().appName("sql_t01").master("local[*]").getOrCreate()
    val data: RDD[String] = spark.sparkContext.textFile("src/spark/data/rdd.txt")
//    data.flatMap(_.split(" ")).foreach(println(_))
    import spark.implicits._
    val raw_df: DataFrame = spark.read.format("csv").option("delimiter", "\t").option("header","true").load("src/spark/data/train.tsv")
    raw_df.printSchema()
    raw_df.createOrReplaceTempView("table_train")
    spark.sql("select * from table_train limit 10").show()


    spark.udf.register("replace_question", (x:String)=>{
      if (x == "?")
        "0"
      else x
    })

  }
}
