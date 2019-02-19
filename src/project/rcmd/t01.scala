package project.rcmd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.Map

/**
  * Created by root on 2019/2/15.
  */
object t01 {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("om").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val data: RDD[String] = sc.textFile("src/project/rcmd/data_t02.txt")
    val dict: Map[String, Long] = data.flatMap(_.split(" ")).map(x=>{x}).distinct().zipWithIndex().collectAsMap()

    val sam = data.map(_.split(" ")).map(features=> {
      val index = features.map(feature=>{
        val rs: Long = dict.get(feature) match {
          case Some(x) => x
          case None => 0
        }
        rs.toInt
      })
//      index
      new SparseVector(dict.size,index,Array.fill(index.length)(1.0))
    })

    sam.foreach(x=>{
     println(x)
    })
  }
}
