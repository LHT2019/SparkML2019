package test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by root on 2019/2/14.
  */
object test_load_data {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("test_load_data").setMaster("local[*]"))
    sc.setLogLevel("WARN")
    val inputdata = MLUtils.loadLibSVMFile(sc, "D:/shangri-la/SparkML2019/data/load_data")
    inputdata.foreach(lable_point=>{
      println(lable_point.label, "---", lable_point.features)
    })
  }
}
