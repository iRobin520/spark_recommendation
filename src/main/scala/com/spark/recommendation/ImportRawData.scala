package com.spark.recommendation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mongodb.client.MongoCollection
import org.apache.spark.sql.types.{BooleanType, IntegerType}
import org.bson.Document

object ImportRawData {

  def main(args: Array[String]): Unit = {
    //Check parameters
    if (args.length<2) {
      System.err.println("Usage: ImportData <Input Sample Data Path> <Input Mongodb connection string>")
      System.exit(0)
    }

    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

    val sampleDataFilePath = args(0)
    val mongodbConnectionString = args(1)
    val collectionName = "spark_user_log"
    val uri = mongodbConnectionString + "." + collectionName
    val conf = new SparkConf()
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)
    val sc = SparkSession.builder()
      .master("local")
      .appName("ImportSampleData")
      .config(conf)
      .getOrCreate()

    val rawDF = sc.read.format("com.databricks.spark.csv")
      .option("header","true") //这里如果在csv第一行有属性的话，没有就是"false"
      .option("inferSchema",true.toString)//这是自动推断属性列的数据类型。
      .load(sampleDataFilePath)//文件的路径
    rawDF.write.format("com.mongodb.spark.sql.DefaultSource")
      .mode("append")
      .option("spark.mongodb.output.uri", uri)
      .save()

    val fullRawDF = MongoSpark.load(sc, ReadConfig(Map("collection" -> collectionName), Some(ReadConfig(sc))))
    //Import products
    //importProductDataFromLog(fullRawDF,mongodbConnectionString)

    //Import ratings
    importRatingDataFromLog(fullRawDF,sc,mongodbConnectionString)
    println("Import sample data success")
    sc.close()
  }

  def importProductDataFromLog(df:org.apache.spark.sql.DataFrame,connectionString:String): Unit = {
    val productDF = df.groupBy("item_id").agg(
      first("category_id"),
      first("brand_id")
    ).toDF()
    productDF.write.format("com.mongodb.spark.sql.DefaultSource")
      .mode("overwrite")
      .option("spark.mongodb.output.uri", connectionString + ".spark_product_info")
      .save()
  }

  def importRatingDataFromLog(df:org.apache.spark.sql.DataFrame,sc:org.apache.spark.sql.SparkSession,connectionString:String): Unit = {
    //Count the viewed product times
    val viewedProductDF = df.filter("action == 0").groupBy("customer_id","item_id").agg(
      count("item_id").as("viewed_count"),
      (first("action")*0).as("added_to_cart_count"),
      (first("action")*0).as("bought_count"),
      (first("action")*0).as("added_to_favorite_count"),
      first("unix_created_at").as("created_at")
    ).toDF()
    //Count the added to cart product times
    val addedToCartProductDF = df.filter("action == 1").groupBy("customer_id","item_id").agg(
      (first("action")*0).as("viewed_count"),
      count("item_id").as("added_to_cart_count"),
      (first("action")*0).as("bought_count"),
      (first("action")*0).as("added_to_favorite_count"),
      first("unix_created_at").as("created_at")
    ).toDF()
    //Count the bought product times
    val boughtProductDF = df.filter("action == 2").groupBy("customer_id","item_id").agg(
      (first("action")*0).as("viewed_count"),
      (first("action")*0).as("added_to_cart_count"),
      count("item_id").as("bought_count"),
      (first("action")*0).as("added_to_favorite_count"),
      first("unix_created_at").as("created_at")
    ).toDF()
    //Count the added to favorite product times
    val addedToFavoriteProductDF = df.filter("action == 3").groupBy("customer_id","item_id").agg(
      (first("action")*0).as("viewed_count"),
      (first("action")*0).as("added_to_cart_count"),
      (first("action")*0).as("bought_count"),
      count("item_id").as("added_to_favorite_count"),
      first("unix_created_at").as("created_at")
    ).toDF()
    //Union all data into one df
    Int
    var totalRelatedProductDF = viewedProductDF.union(addedToCartProductDF)
    totalRelatedProductDF = totalRelatedProductDF.union(boughtProductDF)
    totalRelatedProductDF = totalRelatedProductDF.union(addedToFavoriteProductDF)
    //Calculate the rating value  according to the preset weights
    val productRatingDF = totalRelatedProductDF.groupBy("customer_id","item_id").agg(
      sum("viewed_count").as("viewed_count"),
      sum("added_to_cart_count").as("added_to_cart_count"),
      sum("bought_count").as("bought_count"),
      sum("added_to_favorite_count").as("added_to_favorite_count"),
      ((sum("viewed_count")*0.005*100 + sum("added_to_cart_count").cast(BooleanType).cast(IntegerType)*0.35*100 + sum("bought_count").cast(BooleanType).cast(IntegerType)*0.45*100 + sum("added_to_favorite_count").cast(BooleanType).cast(IntegerType)*0.15*100)/20.0).as("rating"),
      first("created_at")
    ).toDF().cache()
    val ratingDocuments = productRatingDF.rdd.map{
      row =>
        val userId = row.get(0)
        val itemId = row.get(1)
        val viewedCount = row.get(2)
        val addedToCartCount = row.get(3)
        val boughtCount = row.get(4)
        val addedToFavoriteCount = row.get(5)
        val rating = if(row.get(6).asInstanceOf[Double]>5.0) 5.0 else row.get(6) //If the rating > 5.0 then just set it to 5.0
        val createdAt = row.get(7)
        Document.parse(s"{customer_id: $userId,item_id: $itemId, viewed_count: $viewedCount, added_to_cart_count:$addedToCartCount,bought_count:$boughtCount,added_to_favorite_count:$addedToFavoriteCount,rating:$rating,created_at:'$createdAt'}")
    }

    val writeConfig = WriteConfig(Map("collection" -> "spark_rating_info", "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))
    val mongoConnector = MongoConnector(writeConfig.asOptions)
    mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] => collection.drop() })
    MongoSpark.save(ratingDocuments, writeConfig)

    //Import user users
    importUserDataFromRatingData(productRatingDF,connectionString)
  }

  def importUserDataFromRatingData(df:org.apache.spark.sql.DataFrame,connectionString:String): Unit = {
    val purchasedUserDF = df.filter("bought_count>0").groupBy("customer_id").agg(
      concat_ws(",",collect_set("item_id")).as("bought_item_ids")
    ).toDF()
    val notPurchasedUserDF = df.filter("bought_count=0").groupBy("customer_id").agg(
      first("bought_count").as("bought_item_ids")
    ).toDF()
    var allUserDF = purchasedUserDF.union(notPurchasedUserDF)
    allUserDF = allUserDF.groupBy("customer_id").agg(
      first("bought_item_ids").as("bought_item_ids")
    ).toDF()
    allUserDF.show(100)
    allUserDF.write.format("com.mongodb.spark.sql.DefaultSource")
      .mode("overwrite")
      .option("spark.mongodb.output.uri", connectionString + ".spark_user_info")
      .save()
  }

}
