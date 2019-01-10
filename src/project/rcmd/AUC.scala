package project.rcmd

import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, LogisticRegressionModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.mllib.linalg.BLAS.dot

/**
  * Created by Administrator on 2015/12/9.
  */

object AUC {
  //用 LinearDataGenerator.scala类生成数据
  def train(trainPath: String, testPath: String, sc: SparkContext): Unit = {
    val trainData: RDD[LabeledPoint] = MLUtils.loadLabeledPoints(sc, trainPath)
    val testData: RDD[LabeledPoint] = MLUtils.loadLabeledPoints(sc, testPath)
    val model: LogisticRegressionModel = LogisticRegressionWithSGD.train(trainData, 30, 0.8, 1.0)
    val features = testData.map(_.features)
    val labels: RDD[Double] = testData.map(_.label)
    val M = labels.sum().toInt
    val N = labels.count() - M
    val predictLabels: RDD[Double] = model.predict(features)
    val result: RDD[(Double, Double)] = labels.zip(predictLabels)
    val acc = result.filter(x => {
      x._1.equals(x._2)
    }).count() / result.count().toDouble
    println("acc is ===================" + acc)

    val resultPre: RDD[(Double, Double)] = features.map(features => {
      val intercept: Double = model.intercept
      val d1: Vector = model.weights
      val d2: Vector = features
      var sum = 0.0
      for (i <- 0 until model.weights.size){
        sum += d1(i) * d2(i)
      }
      sum += intercept
      val margin = sum
//      val margin = model.weights.dot(features) + intercept
      val score = 1.0 / (1.0 + math.exp(-margin))
      score
    }).zip(labels)
    val orderpre = resultPre.sortBy(_._1, true)
    val totalIndex = orderpre.zipWithIndex().map(data => {
      var index = 0
      if (data._1._2.equals(1.0)) {
        index = data._2.toInt
      }
      index
    }).sum()
    val auc = (totalIndex - (M * (M + 1) / 2)) / (M * N)
    println("auc is ===================" + auc)
  }

  def main(args: Array[String]) {
    val (master, trainPath, testPath) = ("local[4]", args(0), args(1))
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName("LRWithSGD")
      .set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    train(trainPath, testPath, sc)
  }
}
