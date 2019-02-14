package bjsxt.wv

import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by yasaka on 2016/12/25.
  */
object Word2Vector {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("word2vector").setMaster("local")
    val sc = new SparkContext(conf)
    val idData = sc.textFile("doc").map(_.split("[^a-zA-Z]").toSeq).map(_.map(_.toLowerCase)).cache()
    val word2vec = new Word2Vec().setVectorSize(6)
    val model = word2vec.fit(idData)
    val wordVectors = model.getVectors
    wordVectors.foreach{case (word, vector)=>println(word+"\t的词向量是\t"+vector.toSeq)}
    model.findSynonyms("spark",30).foreach(x=>println(x._1))
  }
}
