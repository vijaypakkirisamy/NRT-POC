

package demo

import java.nio.charset.StandardCharsets

import demo.DataStoreConverter.saveRDDtoDataStore
import demo.LimitBreachStreaming.processBreachTags
import demo.LoadtoESCloud.Spark2Es
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions



object LimitBreach {

  def createContext(projectID: String, windowLength: String, slidingInterval: String, checkpointDirectory: String)
    : StreamingContext = {

    // [START stream_setup]
    val sparkConf = new SparkConf().setAppName("LimitBreachtags").set(ConfigurationOptions.ES_NODES, "https://c4b88b17a3194a64b9e04a70b8613e5b.us-central1.gcp.cloud.es.io")
      .set(ConfigurationOptions.ES_PORT, "443")
      .set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "true")
      .set(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "elastic")
      .set(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "Fiw8g63BzNEOeaWYj8ESNY5d")
      .set(ConfigurationOptions.ES_NODES_WAN_ONLY, "true")
      .set(ConfigurationOptions.ES_NODES_INGEST_ONLY, "true")
      .set(ConfigurationOptions.ES_NODES_DATA_ONLY, "true")


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

//process the stream
//    processBreachTags(messagesStream,
//      windowLength.toInt,
//      slidingInterval.toInt,
//      10,
//      //decoupled handler that saves each separate result for processed to datastore
//      saveRDDtoDataStore(_, windowLength.toInt)
//    )

    processBreachTags(messagesStream,
      windowLength.toInt,
      slidingInterval.toInt,
      10,
      //decoupled handler that saves each separate result for processed to datastore
      Spark2Es(_, sc, ssc)
      )

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

    // Create Spark context
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, () => createContext(projectID, windowLength, slidingInterval, checkpointDirectory))

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
