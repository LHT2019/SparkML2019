package bjsxt.rf

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.tree.model.DecisionTreeModel
// 标签：有无出车祸 最后一个唯独有表示车速，连续值
object ClassificationDecisionTree {
  val conf = new SparkConf()
  conf.setAppName("analysItem")
  conf.setMaster("local[3]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    val data = MLUtils.loadLibSVMFile(sc, "汽车数据样本.txt")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))
    //指明类别
    val numClasses=2
    //指定离散变量，未指明的都当作连续变量处理
    //1,2,3,4维度进来就变成了0,1,2,3
    //这里天气维度有3类,但是要指明4,这里是个坑,后面以此类推
    // spark mllib 认为 离散型数据值从0开始，假如第一列值为0,1,2 那么可以写为3，如果是1,2,3那么就需要写4，
    // spark mllib 会认为是0,1,2,3所以写4
    val categoricalFeaturesInfo=Map[Int,Int](0->4,1->4,2->3,3->3)
    //设定评判标准
    val impurity="entropy"
    //树的最大深度,太深运算量大也没有必要
    val maxDepth=5
    //设置离散化程度,连续数据需要离散化,分成几个区间这个参数来,默认其实就是32,分割的区间保证数量差不多
    val maxBins=32
    //生成模型
    val model =DecisionTree.trainClassifier(trainingData,numClasses,categoricalFeaturesInfo,impurity,maxDepth,maxBins)

//    model.save(sc, "")
//    DecisionTreeModel.load(sc, "")
    //测试
   val labelAndPreds = testData.map { point =>
     val prediction = model.predict(point.features)
     (point.label, prediction)
   }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + model.toDebugString)

     }
}
