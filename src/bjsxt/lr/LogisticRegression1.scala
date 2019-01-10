package bjsxt.lr

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yasaka on 2016/12/21.
  */
object LogisticRegression1 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("spark").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val inputData = MLUtils.loadLibSVMFile(sc, "健康状况训练集.txt")
    val splits = inputData.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))
    val lr: LogisticRegressionWithLBFGS = new LogisticRegressionWithLBFGS()
    new LogisticRegressionWithSGD()
    val model = lr.run(trainingData)
    val result = testData
      .map{point=>Math.abs(point.label-model.predict(point.features)) }
    println("正确率="+(1.0-result.mean()))
    println(model.weights.toArray.mkString(" "))
    println(model.intercept)
  }
}
