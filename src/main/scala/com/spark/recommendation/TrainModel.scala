package com.spark.recommendation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark._
import com.mongodb.spark.config._

object TrainModel {
  def main(args: Array[String]): Unit = {

    ///Check parameters
    if (args.length<2) {
      System.err.println("Usage: Recommendation <Input mongodb connection string> <Input model save path(hdfs)>")
      System.exit(0)
    }

    //Filter the unnecessary logs
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

    //Set running environment
    val sparkConf = new SparkConf().setAppName("Train Recommendation Model")
    val sc = new SparkContext(sparkConf)
    val mongodbConnectionString = args(0)
    val modelPath = args(1)

    //exportCSVFilesFromMongoDb(sc,mongodbConnectionString)
    //Load user rating data
    val ratingMongoRdd = sc.loadFromMongoDB(ReadConfig(Map("uri" ->  (mongodbConnectionString + ".spark_rating_info"))))
    //第一步构建time,Rating
    val ratings = ratingMongoRdd.rdd.map(row=>{
      val userId = row.get("customer_id").asInstanceOf[Int]
      val itemId = row.get("item_id").asInstanceOf[Int]
      val rating = row.get("rating").asInstanceOf[Double]
      val timestamp = row.get("created_at").asInstanceOf[String].toLong
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

    bestModel.get.save(sc,modelPath)
    println("Training model complete.")
    println("The trained model has been saved to : " + modelPath)
    sc.stop()
  }

  //第三步
  //定义RMSE方法
  def computeRmse(model:MatrixFactorizationModel,data:RDD[Rating]):Double={
    val predictions:RDD[Rating]=model.predict(data.map(x=>(x.user,x.product)))
    val predictionAndRatings = predictions.map(x=>{((x.user,x.product),x.rating)}).join(data.map(x=>((x.user,x.product),x.rating))).values
    math.sqrt(predictionAndRatings.map(x=>(x._1-x._2)*(x._1-x._2)).mean())
  }
}
