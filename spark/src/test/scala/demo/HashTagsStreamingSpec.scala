
package demo

import demo.LimitBreachStreaming._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class HashTagsStreamingSpec extends WordSpec with MustMatchers with BeforeAndAfter {

  private var sc: SparkContext = _
  private var ssc: StreamingContext = _

  before {
    val conf = new SparkConf().setAppName("unit-testing").setMaster("local")
    ssc = new StreamingContext(conf, Seconds(1))
    sc = ssc.sparkContext
  }

  after {
    if (ssc != null) {
      ssc.stop()
    }
  }


  def getPopularTagsTestHelper(input: List[String], expected: List[Popularity]) = {
    val inputRDD: RDD[String] = sc.parallelize(input)
    val res: Array[Popularity] = extractBreachTags(inputRDD).collect()
    res must have size expected.size
    res.map(_.custid).toList must contain theSameElementsInOrderAs expected.map(_.custid).toList
    res.map(_.custname).toList must contain theSameElementsInOrderAs expected.map(_.custname).toList
    res.map(_.loc).toList must contain theSameElementsInOrderAs expected.map(_.loc).toList
    res.map(_.Trn_amt).toList must contain theSameElementsInOrderAs expected.map(_.Trn_amt).toList
  }

}
