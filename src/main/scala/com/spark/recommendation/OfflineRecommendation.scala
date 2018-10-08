package com.spark.recommendation

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import com.spark.recommendation.utils.MongoClientUtil
import com.spark.recommendation.utils.TimeUtil
import com.spark.recommendation.utils.{MongoClientUtil, TimeUtil}

object OfflineRecommendation {

  def main(args: Array[String]): Unit = {

    //Check parameters
    if (args.length<4) {
      System.err.println("Usage: OfflineRecommendation <Input mongodb host> <Input db name> <Input login name> <Input password> <Name Suffix>")
      System.exit(0)
    }

    //Filter the unnecessary logs
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

    //Set running environment
    //val Array(host, dbName, loginName, password) = args
    val host = args(0)
    val dbName = args(1)
    val loginName = args(2)
    val password = args(3)
    var nameSuffix = ""
    if (args.length>4) {
      nameSuffix = args(4)
    }

    val connectionString = "mongodb://" + loginName + ":" + password + "@" + host + ":27017/" + dbName
    val uri = connectionString + ".spark_rating_info"
    val sparkConf = new SparkConf()
      .setAppName("Make Offline Recommendations")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)
      .set("spark.driver.maxResultSize", "5g")
      .set("spark.driver.memory", "8g")
    val sc = new SparkContext(sparkConf)
    val ss = SparkSession.builder()
      .master("local")
      .config(sparkConf)
      .getOrCreate()

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
    val ratingDF = ratingMongoRdd.toDF().select("user_id","item_id","rating","created_at","updated_at").where(filterConditions)
    val ratings = ratingDF.rdd.map(row=>{
      val userId = row.get(0).asInstanceOf[Int]
      val itemId = row.get(1).asInstanceOf[Int]
      val rating = row.get(2).asInstanceOf[Double]
      val timestamp = row.get(3).asInstanceOf[String].toLong
      (timestamp % 10, Rating(userId, itemId, rating))
    })

    //输出数据的基本信息
    val numRatings  = ratings.count()
    val numUser  = ratings.map(_._2.user).distinct().count()
    val numItems = ratings.map(_._2.product).distinct().count()
    println("样本基本信息为：")
    println("样本数："+numRatings)
    println("用户数："+numUser)
    println("物品数："+numItems)

    //第二步骤
    //将样本评分表以key值切分成3个部分，分别用于训练 (60%，并加入用户评分), 校验 (20%), and 测试 (20%)
    //使用日期把数据分为训练集(timestamp<6),验证集(6<timestamp<8)和测试集（timestamp>8）

    val numPartitions = 4
    val allData = ratings.values.repartition(numPartitions)
    val training = ratings.filter(x => x._1 < 6).values.repartition(numPartitions).cache()
    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8).values.repartition(numPartitions).cache()
    val test = ratings.filter(x => x._1 >= 8).values.cache()

    val numTraining = training.count()
    val numValidation=validation.count()
    val numTest=test.count()

    println("验证样本基本信息为：")
    println("训练样本数："+numTraining)
    println("验证样本数："+numValidation)
    println("测试样本数："+numTest)

    //第四步骤，使用不同的参数训练模型，并且选择RMSE最小的模型，规定参数的范围
    //隐藏因子数：8或者12
    //正则化系数，0.01或者0.1选择，迭代次数为10或者20,训练8个模型
    val ranks = List(8,12)
    val lambdas = List(0.01,0.1)
    val numIters = List(10,20)
    var bestModel:Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLamdba = -1.0
    var bestNumIter = 1
    var startTime = System.nanoTime()
    for(rank<-ranks;lambda<-lambdas;numIter<-numIters){
      val model = ALS.train(allData,rank,numIter,lambda)
      val validationRmse = computeRmse(model,validation)
      println("RMSE(validation) = " + validationRmse + " for the model trained with rank = "
        + rank + ",lambda = " + lambda + ",and numIter = " + numIter + ".")
      if(validationRmse < bestValidationRmse){
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLamdba = lambda
        bestNumIter = numIter
      }
    }

    val testRmse = computeRmse(bestModel.get,test)
    println("测试数据的rmse为："+testRmse)
    println("范围内的最后模型参数为：")
    println("隐藏因子数："+bestRank)
    println("正则化参数："+bestLamdba)
    println("迭代次数："+bestNumIter)

    //步骤5可以对比使用协同过滤和不适用协同过滤（使用平均分来做预测结果）能提升多大的预测效果。
    //计算训练样本和验证样本的平均分数
    val meanR = training.union(validation).map(x=>x.rating).mean()
    //这就是使用平均分做预测，test样本的rmse
    val baseRmse=math.sqrt(test.map(x=>(meanR-x.rating)*(meanR-x.rating)).mean())
    val improvement =(baseRmse-testRmse)/baseRmse*100
    println("使用了ALS协同过滤算法比使用评价分作为预测的提升度为："+improvement)

    var consumingTime = System.nanoTime() - startTime
    println("------------------------------")
    println("Training consuming:" + consumingTime / 1000000000 + "s")

    println("----------Make recommendation start-------------------")

    //Initialize the collection data of spark_recommendation
    val fields = Array(
      StructField("user_id", IntegerType, nullable = true),
      StructField("item_id", IntegerType, nullable = true),
      StructField("rating", DoubleType, nullable = true),
      StructField("created_at", LongType, nullable = true)
    )
    val schema = StructType(fields)
    val collectionName = "spark_recommendation" + nameSuffix
    println("collection Name--------------------------" + collectionName)
    var fullRecommendationDF = ss.createDataFrame(sc.emptyRDD[Row],schema)
    fullRecommendationDF.write.format("com.mongodb.spark.sql.DefaultSource")
    fullRecommendationDF.write.format("com.mongodb.spark.sql.DefaultSource")
      .mode("overwrite")
      .option("spark.mongodb.output.uri", connectionString + "." + collectionName)
      .save()
    mongoClientUtil.createIndex("user_id","user_id_index",collectionName)
    mongoClientUtil.createIndex("item_id","item_id_index",collectionName)

    var counter = 0
    //Recommend 200 products for all users,
    bestModel.get.recommendProductsForUsers(200).repartition(10).map(x => {
      val userId = x._1.toInt
      val rat = x._2
      var recommendations = ""
      for( item <- rat){
        val itemId = item.product
        val rating = item.rating
        recommendations = recommendations + userId + "-" + itemId + "-" + rating + ","
      }
      recommendations = recommendations.substring(0,recommendations.length-2)
      recommendations
    }).collect().foreach(line=>{
      val rows = line.split(",").map(item => {
        val elements = item.split("-")
        val userId = elements(0).toInt
        val itemId = elements(1).toInt
        val rating = elements(2).toDouble
        val createdAt = TimeUtil.getCurrentTime()
        Row(userId,itemId,rating,createdAt)
      })
      val recommendationRDD = sc.parallelize(rows)
      val recommendationDF = ss.createDataFrame(recommendationRDD,schema)
      if (counter>100) { //20000 rows for each writing
        fullRecommendationDF.write.format("com.mongodb.spark.sql.DefaultSource")
        fullRecommendationDF.write.format("com.mongodb.spark.sql.DefaultSource")
          .mode("append")
          .option("spark.mongodb.output.uri", connectionString + "." + collectionName)
          .save()
        fullRecommendationDF = ss.createDataFrame(sc.emptyRDD[Row],schema)
        counter = 0
      }
      fullRecommendationDF = fullRecommendationDF.union(recommendationDF)
      counter = counter + 1
    })
    fullRecommendationDF.write.format("com.mongodb.spark.sql.DefaultSource")
      .mode("append")
      .option("spark.mongodb.output.uri", connectionString + "." + collectionName)
      .save()

    println("----------Make recommendation completed-------------------")
    ss.close()
    sc.stop()
  }

  def makeRecommendation(model:MatrixFactorizationModel,sc:SparkContext, ss:SparkSession, connection:String): Unit = {
    //Initialize the collection data of spark_recommendation
    val fields = Array(
      StructField("user_id", IntegerType, nullable = true),
      StructField("item_id", IntegerType, nullable = true),
      StructField("rating", DoubleType, nullable = true)
    )
    val schema = StructType(fields)
    var fullRecommendationDF = ss.createDataFrame(sc.emptyRDD[Row],schema)
    fullRecommendationDF.write.format("com.mongodb.spark.sql.DefaultSource")
    fullRecommendationDF.write.format("com.mongodb.spark.sql.DefaultSource")
      .mode("overwrite")
      .option("spark.mongodb.output.uri", connection + ".spark_recommendation_v2")
      .save()

    var counter = 0
    //Recommend 200 products for all users,
    //val recommendations = ExtMatrixFactorizationModelHelper.recommendProductsForUsers( model, 10, 10000, StorageLevel.MEMORY_AND_DISK_SER )
    model.recommendProductsForUsers(300).repartition(10).map(x => {
      val userId = x._1.toInt
      val rat = x._2
      var recommendations = ""
      for( item <- rat){
        val itemId = item.product
        val rating = item.rating
        recommendations = recommendations + userId + "-" + itemId + "-" + rating + ","
      }
      recommendations = recommendations.substring(0,recommendations.length-2)
      recommendations
    }).collect().foreach(line=>{
        val rows = line.split(",").map(item => {
        val elements = item.split("-")
        val userId = elements(0).toInt
        val itemId = elements(1).toInt
        val rating = elements(2).toDouble
        Row(userId,itemId,rating)
      })
      val recommendationRDD = sc.parallelize(rows)
      val recommendationDF = ss.createDataFrame(recommendationRDD,schema)
      if (counter>100) { //20000 rows for each writing
        fullRecommendationDF.write.format("com.mongodb.spark.sql.DefaultSource")
        fullRecommendationDF.write.format("com.mongodb.spark.sql.DefaultSource")
          .mode("append")
          .option("spark.mongodb.output.uri", connection + ".spark_recommendation")
          .save()
        fullRecommendationDF = ss.createDataFrame(sc.emptyRDD[Row],schema)
        counter = 0
      }
      fullRecommendationDF = fullRecommendationDF.union(recommendationDF)
      counter = counter + 1
    })
    fullRecommendationDF.write.format("com.mongodb.spark.sql.DefaultSource")
      .mode("append")
      .option("spark.mongodb.output.uri", connection + ".spark_recommendation")
      .save()
  }

  //定义RMSE方法
  def computeRmse(model:MatrixFactorizationModel,data:RDD[Rating]):Double={
    val predictions:RDD[Rating]=model.predict(data.map(x=>(x.user,x.product)))
    val predictionAndRatings = predictions.map(x=>{((x.user,x.product),x.rating)}).join(data.map(x=>((x.user,x.product),x.rating))).values
    math.sqrt(predictionAndRatings.map(x=>(x._1-x._2)*(x._1-x._2)).mean())
  }

}
