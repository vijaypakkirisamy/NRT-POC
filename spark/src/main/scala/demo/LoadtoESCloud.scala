package demo

import demo.LimitBreachStreaming.Popularity
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.streaming.EsSparkStreaming

import scala.collection.Seq

object LoadtoESCloud {

def Spark2Es (tags: Array[Popularity], windowLength: Int) {

  val conf = new SparkConf().setAppName("LimitBreach").set(ConfigurationOptions.ES_NODES, "https://c4b88b17a3194a64b9e04a70b8613e5b.us-central1.gcp.cloud.es.io")
    .set(ConfigurationOptions.ES_PORT, "443")
    .set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "true")
    .set(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "elastic")
    .set(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "Fiw8g63BzNEOeaWYj8ESNY5d")

  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(windowLength.toInt))

  val rdd = sc.makeRDD(Seq(tags))
  val microbatches = scala.collection.mutable.Queue(rdd)
   val dstream = ssc.queueStream(microbatches)
   EsSparkStreaming.saveToEs(dstream, "breach")
    println("Data Pumped to ElasticSearch Cluster.. Please check..")

}
}
