

package demo

import com.google.cloud.Timestamp
import com.google.cloud.datastore._
import demo.LimitBreachStreaming.Popularity

object DataStoreConverter {

  private def convertToDatastore(keyFactory: KeyFactory,
                                 record: Popularity): FullEntity[IncompleteKey] =
    FullEntity.newBuilder(keyFactory.newKey())
      .set("custidentifier", record.custid)
      .set("custname", record.custname)
      .set("txnamt", record.Trn_amt)
      .set("location", record.loc)
      .build()

  // [START convert_identity] 
  private[demo] def convertToEntity(hashtags: Array[Popularity],
                                    keyFactory: String => KeyFactory): FullEntity[IncompleteKey] = {
    val hashtagKeyFactory: KeyFactory = keyFactory("Hashtag")

    val listValue = hashtags.foldLeft[ListValue.Builder](ListValue.newBuilder())(
      (listValue, hashTag) => listValue.addValue(convertToDatastore(hashtagKeyFactory, hashTag))
    )

    val rowKeyFactory: KeyFactory = keyFactory("LimitBreaches")

    FullEntity.newBuilder(rowKeyFactory.newKey())
      .set("datetime", Timestamp.now())
      .set("hashtags", listValue.build())
      .build()
  }
  // [END convert_identity]

  def saveRDDtoDataStore(tags: Array[Popularity],
                         windowLength: Int): Unit = {
    val datastore: Datastore = DatastoreOptions.getDefaultInstance().getService()
    val keyFactoryBuilder = (s: String) => datastore.newKeyFactory().setKind(s)

    val entity: FullEntity[IncompleteKey] = convertToEntity(tags, keyFactoryBuilder)

    datastore.add(entity)

    // Display some info in the job's logs
    println("\n-------------------------")
    println(s"Window ending ${Timestamp.now()} for the past ${windowLength} seconds\n")
    if (tags.length == 0) {
      println("No Limit Breaches in this window.")
    }
    else {
      println("Limit Breaches in this window:")
      tags.foreach(hashtag => println(s"${hashtag.custname}, ${hashtag.custid}, ${hashtag.Trn_amt}"))
    }
  }



}
