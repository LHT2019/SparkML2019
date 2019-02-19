package bjsxt.kmeans

import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.Level
import org.apache.log4j.Logger

/**
  * Created by yasaka on 2017/8/14.
  * 1 1996
  * 2 648
  * 3 0.18
  * 4 0.14
  */
object KMeans1 {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("KMeans1").setMaster("local")
    val sc = new SparkContext(conf)
    val data: RDD[String] = sc.textFile("kmeans_data.txt")
    val parsedData: RDD[Vector] = data.map(line => Vectors.dense(line.split(' ').map(_.toDouble)))

    val numClusters = 4
    val numIterations = 5
    val model: KMeansModel = KMeans.train(parsedData, numClusters, numIterations)
    model.clusterCenters.foreach(println(_))

//    对已经存在的点进行判断
    println(model.predict(Vectors.dense("0.2 0.2 0.2".split(' ').map(_.toDouble))))
//    对不存在的点进行判断
    println(model.predict(Vectors.dense("8 8 8".split(' ').map(_.toDouble))))

//    评估平方误差
    val WSSSE = model.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

  }
}
