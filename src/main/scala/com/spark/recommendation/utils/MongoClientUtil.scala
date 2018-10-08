package com.spark.recommendation.utils

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb._
import com.mongodb.DBObject
import com.mongodb.casbah.{MongoClient, MongoCredential, MongoDB}



class MongoClientUtil(host:String,dbName:String,loginName:String,password:String) {
  private val mongoDB:MongoDB = createDatabase(host, 27017, dbName, loginName, password)

  def getCollection(collectionName:String): DBCollection ={
    mongoDB.getCollection(collectionName)
  }

  def findByMatchFields(conditions:List[(String, Any)],collectionName:String): DBCursor = {
    val query = MongoDBObject(conditions)
    //val query = MongoDBObject("customer_id" -> 12)
    getCollection(collectionName).find(query)
  }

  def findByOperators(conditions:List[(String,String,Any)],collectionName:String): DBCursor ={
    var query:BasicDBObject = null
    conditions.foreach(item=>{
      val fieldName = item._1
      val operator = item._2
      val value = item._3
      if (query == null) {
        query = new BasicDBObject(fieldName,new BasicDBObject(operator, value))
      } else {
        query.append(fieldName,new BasicDBObject(operator, value))
      }
    })
    getCollection(collectionName).find(query)
  }

  def delete(conditions:List[(String, Any)],collectionName:String): Unit = {
    val query = MongoDBObject(conditions)
    getCollection(collectionName).remove(query)
  }

  def createIndex(key:String,name:String,collectionName:String): Unit = {
    getCollection(collectionName).createIndex(MongoDBObject(key->1),name)
  }

  def deleteAll(collectionName:String): Unit = {
    val query = new BasicDBObject()
    getCollection(collectionName).remove(query)
  }

  def update(conditions:List[(String, Any)],data:List[(String, Any)],collectionName:String): Unit = {
    val query = MongoDBObject(conditions)
    val value = MongoDBObject(data)
    getCollection(collectionName).update(query,value)
  }

  def insert(data:List[(String, Any)],collectionName:String): Unit = {
    val insertData = MongoDBObject(data)
    getCollection(collectionName).insert(insertData)
  }

  def createDatabase(url: String, port: Int, dbName: String, loginName: String, password: String): MongoDB = {
    val server = new ServerAddress(url, port)
    //注意：MongoCredential中有6种创建连接方式，这里使用MONGODB_CR机制进行连接。如果选择错误则会发生权限验证失败
    val credentials = MongoCredential.createCredential(loginName, dbName, password.toCharArray)
    val mongoClient = MongoClient(server, List(credentials))
    mongoClient.getDB(dbName)
  }

  def createDatabase(url: String, port: Int, dbName: String): MongoDB = {
    MongoClient(url, port).getDB(dbName)
  }
}
