package test

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2019/1/10.
  */
object test_spark {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test_spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val data = sc.textFile("./data/wc.txt").cache()
    data.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(
      word =>{
        println(word._1 + " --- " + word._2)
      }
    )
  }
}
