package com.spark.recommendation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark._
import com.mongodb.spark.config._
import com.spark.recommendation.utils.MongoClientUtil
import com.spark.recommendation.utils.{MongoClientUtil, TimeUtil}

object Recommendation {

  def main(args: Array[String]): Unit = {

    //Check parameters
    if (args.length<4) {
      System.err.println("Usage: Recommendation <Input mongodb host> <Input db name> <Input login name> <Input password>")
      System.exit(0)
    }

    //Filter the unnecessary logs
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

    val Array(host, dbName, loginName, password) = args

    println("----------------24hours ago:"+TimeUtil.get24HoursAgoTime())

    //Set running environment
    val uri = "mongodb://" + loginName + ":" + password + "@" + host + ":27017/" + dbName + ".spark_rating_info"
    val sparkConf = new SparkConf().setAppName("Make Recommendations").set("spark.driver.maxResultSize", "5g").set("spark.driver.memory", "8g")
    val sc = new SparkContext(sparkConf)

    val mongoClientUtil = new MongoClientUtil(host,dbName,loginName,password)
    val filterItems = mongoClientUtil.findByMatchFields(List(("is_available",1)),"spark_filter_setting")
    var filterItemIds = List[Int]()
    var filterCatIds = List[Int]()
    var filterBrandIds = List[Int]()
    while (filterItems.hasNext) {
      val item = filterItems.next()
      if (item.get("object_type") == "Product") {
        filterItemIds=filterItemIds:+item.get("object_id").asInstanceOf[Int]
      } else if (item.get("object_type") == "Category") {
        filterCatIds=filterCatIds:+item.get("object_id").asInstanceOf[Int]
      } else if (item.get("object_type") == "Brand") {
        filterBrandIds=filterBrandIds:+item.get("object_id").asInstanceOf[Int]
      }
    }

    val itemIds = filterItemIds.map(id=>"'"+id+"'").mkString(",")
    val catIds = filterCatIds.map(id=>"'"+id+"'").mkString(",")
    val brandIds = filterBrandIds.map(id=>"'"+id+"'").mkString(",")

    var filterConditions = "1 = 1"
    if (itemIds.length>0) {
      filterConditions = filterConditions + " AND item_id NOT IN ("+itemIds+")"
    }
    if (catIds.length>0) {
      filterConditions = filterConditions + " AND cat_id NOT IN ("+catIds+")"
    }
    if (brandIds.length>0) {
      filterConditions = filterConditions + " AND brand_id NOT IN ("+brandIds+")"
    }

    println("---------conditions:" + filterConditions)

    val ratingMongoRdd = sc.loadFromMongoDB(ReadConfig(Map("uri" ->  uri)))
    val ratingDF = ratingMongoRdd.toDF().select("user_id","item_id","rating","updated_at").where(filterConditions)
    val ratings = ratingDF.rdd.map(row=>{
      val userId = row.get(0).asInstanceOf[Int]
      val itemId = row.get(1).asInstanceOf[Int]
      val rating = row.get(2).asInstanceOf[Double]
      Rating(userId, itemId, rating)
    })

    println("----------Make recommendation start-------------------")

    val numPartitions = 4
    val bestRank = 12
    val bestNumIter = 20
    val bestLambda = 0.01
    val allData = ratings.repartition(numPartitions)

    var startTime = System.nanoTime()
    val model = ALS.train(allData,bestRank,bestNumIter,bestLambda)
    var consumingTime = System.nanoTime() - startTime
    println("------------------------------")
    println("Training consuming:" + consumingTime / 1000000000 + "s")

    startTime = System.nanoTime()

//    val userId = 12
//    val recommendations = model.recommendProducts(userId,100)
//    if (recommendations.length>0) {
//      recommendations.foreach(println)
//    }

    val distinctUsersDF = ratingDF.select("user_id").where("updated_at>="+TimeUtil.get24HoursAgoTime()).dropDuplicates("user_id")
    println("--------total count:"+distinctUsersDF.count())
    distinctUsersDF.collect().foreach(row => {
      val userId = row.get(0).asInstanceOf[Int]
      val recommendations = model.recommendProducts(userId,300)
      if (recommendations.length>0) {
        mongoClientUtil.delete(List(("user_id",userId.toString)),"spark_recommendation")
        recommendations.foreach(item=>{
          val userId = item.user
          val itemId = item.product
          val rating = item.rating
          val createdAt = TimeUtil.getCurrentTime()
          mongoClientUtil.insert(List(("user_id",userId),("item_id",itemId),("rating",rating),("created_at",createdAt)),"spark_recommendation")
        })
      }
    })

    consumingTime = System.nanoTime() - startTime
    println("Recommendation consuming:" + consumingTime / 1000000000 + "s")

    println("----------Make recommendation completed-------------------")
    sc.stop()
  }
}
