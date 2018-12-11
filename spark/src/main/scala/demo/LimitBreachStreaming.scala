

package demo

import com.fasterxml.jackson.module.scala.util.Strings
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream


object LimitBreachStreaming {
  case class Popularity(custid: Int, custname: String, loc: String, Trn_amt: Int)
 // case class Popularity(tag: String, amount: Int)
  // [START extract]
  private[demo] def extractBreachTags(input: RDD[String]): RDD[Popularity] =
    input.flatMap( r => r.split(","))
            .map(x => Popularity(x.toInt, x(1).toString, x(2).toString, x(3).toInt))
            .filter(y => y.Trn_amt > 550000)

  /*input.flatMap(_.split("\\s+")) // Split on any white character
    .filter(_.startsWith("#")) // Keep only the hashtags
    // Remove punctuation, force to lowercase
    .map(_.replaceAll("[,.!?:;]", "").toLowerCase)
    // Remove the first #
    .map(_.replaceFirst("^#", ""))
    .filter(!_.isEmpty) // Remove any non-words
    .map((_, 1)) // Create word count pairs
    .reduceByKey(_ + _) // Count occurrences
    .map(r => Popularity(r._1, r._2))
    // Sort hashtags by descending number of occurrences
    .sortBy(r => (-r.amount, r.tag), ascending = true)
  // [END extract] */

  def processBreachTags(input: DStream[String],
                              windowLength: Int,
                              slidingInterval: Int,
                              n: Int,
                              handler: Array[Popularity] => Unit): Unit = {
    val sortedHashtags: DStream[Popularity] = input
      .window(Seconds(windowLength), Seconds(slidingInterval))
      .transform(extractBreachTags(_))

    sortedHashtags.foreachRDD(rdd => {
      handler(rdd.take(n))
    })
  }

}
