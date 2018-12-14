package demo

import demo.LimitBreachStreaming.Popularity
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.elasticsearch.spark.streaming.EsSparkStreaming

import scala.collection.Seq

object LoadtoESCloud {

def Spark2Es (tags: Array[Popularity], sc: SparkContext, ssc: StreamingContext) {
  val rdd = sc.makeRDD(Seq(tags))
  val microbatches = scala.collection.mutable.Queue(rdd)
   val dstream = ssc.queueStream(microbatches)
   EsSparkStreaming.saveToEs(dstream, "breaches/trandtls")
    println("Data Pumped to ElasticSearch Cluster.. Please check..")
  }
}
