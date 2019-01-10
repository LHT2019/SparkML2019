package bjsxt.lr

import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yasaka on 2016/12/21.
  */
object LogisticRegression4 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("spark").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val inputData = MLUtils.loadLibSVMFile(sc, "健康状况训练集.txt")
    val splits = inputData.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))
    val lr = new LogisticRegressionWithLBFGS()
    lr.setIntercept(true)
//    val model = lr.run(trainingData)
//    val result = testData
//      .map{point=>Math.abs(point.label-model.predict(point.features)) }
//    println("正确率="+(1.0-result.mean()))
//    println(model.weights.toArray.mkString(" "))
//    println(model.intercept)

    val model = lr.run(trainingData).clearThreshold()
    val errorRate = testData.map{p=>
      val score = model.predict(p.features)
      // 癌症病人宁愿判断出得癌症也别错过一个得癌症的病人
      val result = score>0.4 match {case true => 1 ; case false => 0}
      Math.abs(result-p.label)
    }.mean()
    println(1-errorRate)
  }
}
