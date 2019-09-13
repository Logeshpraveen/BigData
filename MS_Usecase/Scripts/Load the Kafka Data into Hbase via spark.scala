
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{ DStream, InputDStream }
import org.apache.spark.streaming.kafka.v09.KafkaUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.SQLContext

object PurchaseData extends Serializable {

  // schema for purchase data   
  case class purchase(session_id: String, event: String, partner_id: String, partner_name: String, cart_amount: Double, 
Country: String, user_agent: String, user_id: String, version: String, language: String, date: String, search_query: String,
current_url: String, category: String, referrer: String, init_session: String,page_type: String) extends Serializable

  // function to purchase Data into purchase class
  def pdata(str: String): purchase = {
    val p = str.split(",")
    purchase(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8),p(9),p(10),p(11),p(12),p(13),p(14),p(15))
  }
  val timeout = 10 // Terminate after N seconds
  val batchSeconds = 2 // Size of batch intervals

  def main(args: Array[String]): Unit = {

    val brokers = "kafkademo:9092" 
    val groupId = "testgroup"
    val offsetReset = "earliest"
    val batchInterval = "2"
    val pollTimeout = "1000"
    val topics = "/user/user01/purchase"

    val sparkConf = new SparkConf().setAppName("Purchased Data")

    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval.toInt))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      "spark.kafka.poll.time" -> pollTimeout
    )

    val messages = KafkaUtils.createDirectStream[String, String](ssc, kafkaParams, topicsSet)

    val purchaseDStream = messages.map(_._2).map(pData)

    purchaseDStream.foreachRDD { rdd =>
     if (!rdd.isEmpty) {
        val count = rdd.count
        println("count received " + count)
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._
        import org.apache.spark.sql.functions._

        val purchaseDF = rdd.toDF()
        purchaseDF.show()
        purchaseDF.registerTempTable("purchase")
        val resDF = sqlContext.sql("SELECT * FROM purchase")
        resDF.show

      	resDF.write.options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
 	.format("org.apache.hadoop.hbase.spark ").save()  
      }
    }
    // Start the computation
    println("start streaming")
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()

  }

}