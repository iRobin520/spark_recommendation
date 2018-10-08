package com.spark.recommendation

import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.mongodb.spark._
import com.mongodb.spark.config._
import java.util.HashMap

import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import com.mongodb.client.MongoCollection
import com.mongodb.client.model._
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.bson.Document

object ProcessLogData {
  private val DefaultMaxBatchSize = 1024
  implicit val formats = Serialization.formats(ShortTypeHints(List()))

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("Usage: ProcessLogData <brokers> <topics> <mongodb>")
      System.exit(1)
    }
    //Filter the unnecessary logs
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)


    val Array(brokers, topics, mongodb) = args
    val collectionName = "spark_rating_info"
    val uri = mongodb + "." + collectionName
    val sparkConf = new SparkConf()
      .setAppName("ProcessLogData")
      .set("spark.driver.maxResultSize", "5g")
      .set("spark.driver.memory", "8g")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)

    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("checkpoint")
    val ss = SparkSession.builder()
      .master("local")
      .config(sparkConf)
      .getOrCreate()

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> "com.sh.wcc",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    val ratingInfoDF = MongoSpark.load(ss, ReadConfig(Map("collection" -> collectionName), Some(ReadConfig(ss)))).toDF().select("customer_id","item_id","rating")
    val ratings = ratingInfoDF.rdd.map(row=>{
      val userId = row.get(0).asInstanceOf[Int]
      val itemId = row.get(1).asInstanceOf[Int]
      val rating = row.get(2).asInstanceOf[Double]
      Rating(userId, itemId, rating)
    })
    val numPartitions = 4
    val bestRank = 12
    val bestNumIter = 20
    val bestLambda = 0.01
    var allData = ratings.repartition(numPartitions)

    // Get the lines, split them into words, count the words and print
    messages.map(_.value).foreachRDD(rdd => {
      if (rdd.count()>0) {
        val startTime = System.nanoTime()
        val newData = ss.read.json(rdd).rdd.map(row=>
        {
          val userId = row.get(0).asInstanceOf[String].toInt
          val itemId = row.get(1).asInstanceOf[String].toInt
          val rating = row.get(2).asInstanceOf[Double]
          Rating(userId, itemId, rating)
        })
        newData.collect().foreach(println)
        allData = allData.union(newData)

        val model = ALS.train(allData,bestRank,bestNumIter,bestLambda)
        val consumingTime = System.nanoTime() - startTime
        println("------------------------------")
        println("Training consuming:" + consumingTime / 1000000000 + "s")
        newData.collect().foreach(item => {
          val userId = item.user
          val recommendations = model.recommendProducts(userId,200)
          sendMessageToKafka(recommendations)
        })
        println("Recommend Complete.")
        println("------------------------------")
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def sendMessageToKafka(data: Array[Rating]): Unit ={
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    // 实例化一个Kafka生产者
    val producer = new KafkaProducer[String, String](props)
    val strValues = write(data)
    // 封装成Kafka消息，topic为"result"
    val message = new ProducerRecord[String, String]("RecommendResult", null, strValues)
    // 给Kafka发送消息
    producer.send(message)
  }

  def saveAndUpdate(rdd: RDD[Document], writeConfig: WriteConfig, date:String): Unit = {
    val mongoConnector = MongoConnector(writeConfig.asOptions)                                                       // Gets the connector
    rdd.foreachPartition(iter => if (iter.nonEmpty) {
      mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>                        // The underlying Java driver MongoCollection instance.
        val updateOptions = new UpdateOptions().upsert(true)
        iter.grouped(DefaultMaxBatchSize).foreach(batch =>
          // Change this block to update / upsert as needed
          batch.map{doc=>
            collection.updateOne(new Document("date", date), new Document("$set", doc), updateOptions)
          })
      })
    })
  }
}
