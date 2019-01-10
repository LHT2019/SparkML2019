package bjsxt.lr

import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yasaka on 2016/12/21.
  */
object LogisticRegression6 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("spark").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val inputData = MLUtils.loadLibSVMFile(sc, "环境分类数据.txt")

    val vectors = inputData.map(_.features)
    val scalerModel = new StandardScaler(withMean=true, withStd=true).fit(vectors)
    val normalizeInputData = inputData.map{point =>
      val label: Double = point.label
      val features: Vector = scalerModel.transform(point.features.toDense)
      new LabeledPoint(label,features)
    }

    val splits = normalizeInputData.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))
    val lr=new LogisticRegressionWithLBFGS()
    lr.setIntercept(true)
    val model = lr.run(trainingData)
    val result=testData
      .map{point=>Math.abs(point.label-model.predict(point.features)) }
    println("正确率="+(1.0-result.mean()))
    println(model.weights.toArray.mkString(" "))
    println(model.intercept)
  }
}
