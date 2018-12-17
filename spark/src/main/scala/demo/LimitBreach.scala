

package demo

import java.nio.charset.StandardCharsets

import demo.DataStoreConverter.saveRDDtoDataStore
import demo.LimitBreachStreaming.{Popularity, processBreachTags}
import demo.LoadtoESCloud.Spark2Es
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.streaming.EsSparkStreaming

import scala.collection.Seq




object LimitBreach {

  def createContext(projectID: String, windowLength: String, slidingInterval: String, checkpointDirectory: String)
  : StreamingContext = {

    // [START stream_setup]
    val sparkConf = new SparkConf().setAppName("LimitBreachtags").set(ConfigurationOptions.ES_NODES, "c4b88b17a3194a64b9e04a70b8613e5b.us-central1.gcp.cloud.es.io")
      .set(ConfigurationOptions.ES_PORT, "443")
      .set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "true")
      .set(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "elastic")
      .set(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "Fiw8g63BzNEOeaWYj8ESNY5d")

      //.set(ConfigurationOptions.ES_NODES_WAN_ONLY, "true")
     // .set(ConfigurationOptions.ES_NODES_INGEST_ONLY, "true")
   //   .set(ConfigurationOptions.ES_NODES_DATA_ONLY, "true")
     // .set(ConfigurationOptions.ES_NODES_DISCOVERY, "true")
     // .set(ConfigurationOptions.ES_NODES_CLIENT_ONLY, "false")


    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(slidingInterval.toInt))

    // Set the checkpoint directory
    val yarnTags = sparkConf.get("spark.yarn.tags")
    val jobId = yarnTags.split(",").filter(_.startsWith("dataproc_job")).head
    ssc.checkpoint(checkpointDirectory + '/' + jobId)

    // Create stream
    val messagesStream: DStream[String] = PubsubUtils
      .createStream(
        ssc,
        projectID,
        None,
        "limits-subscription",
        SparkGCPCredentials.builder.build(), StorageLevel.MEMORY_AND_DISK_SER_2)
      .map(message => new String(message.getData(), StandardCharsets.UTF_8))
    // [END stream_setup]

    println("Stage 2 is :" + messagesStream.print())

    //    process the stream
    //    processBreachTags(messagesStream,
    //      windowLength.toInt,
    //      slidingInterval.toInt,
    //      10,
    //      //decoupled handler that saves each separate result for processed to datastore
    //      saveRDDtoDataStore(_, windowLength.toInt)
    //    )

    //    processBreachTags(messagesStream,
    //      windowLength.toInt,
    //      slidingInterval.toInt,
    //      10,
    //      //decoupled handler that saves each separate result for processed to datastore
    //      Spark2Es(_, sc, ssc)
    //      )

    val mstream: DStream[Popularity] = messagesStream.map(x => x.split(" "))
      .map(x => Popularity(x(0).toInt, x(1).toString, x(2).toString, x(3).toInt))
      .filter(y => y.Trn_amt > 550000)

    println("Stage 3 is :" + mstream.print())

    val rdd = sc.makeRDD(Seq(mstream))
    val microbatches = scala.collection.mutable.Queue(rdd)
    val dstream = ssc.queueStream(microbatches)
    EsSparkStreaming.saveToEs(dstream, "breaches/trandtls")
    println("Stage 4 is Data Pumped to ElasticSearch Cluster.. Please check..")

    ssc

}


  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      System.err.println(
        """
          | Usage: TrendingHashtags <projectID> <windowLength> <slidingInterval> <totalRunningTime>
          |
          |     <projectID>: ID of Google Cloud project
          |     <windowLength>: The duration of the window, in seconds
          |     <slidingInterval>: The interval at which the window calculation is performed, in seconds
          |     <totalRunningTime>: Total running time for the application, in minutes. If 0, runs indefinitely until termination.
          |     <checkpointDirectory>: Directory used to store RDD checkpoint data
          |
        """.stripMargin)
      System.exit(1)
    }

    val Seq(projectID, windowLength, slidingInterval, totalRunningTime, checkpointDirectory) = args.toSeq

    println("Stage 1")
    // Create Spark context
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, () => createContext(projectID, windowLength, slidingInterval, checkpointDirectory))

    println("Stage 6")
    // Start streaming until we receive an explicit termination
    ssc.start()

    if (totalRunningTime.toInt == 0) {
      ssc.awaitTermination()
    }
    else {
      ssc.awaitTerminationOrTimeout(1000 * 60 * totalRunningTime.toInt)
    }
  }

}
