/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.connector.example


import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.asterix._
import org.apache.asterix.connector._
import org.apache.spark.sql.types._

/**
 * To Run this example, you need to have AsterixDB up and running.
 *
 * The frame size is specified at conf/asterix-configuration.xml file under Managix folder.
 * Default value of the frame size is 131072.
 *
 * If you're running AsterixDB using [[AsterixHyracksIntegrationUtil]] usually the
 * frame size is 32768.
 *
 * If you don't know what [[AsterixHyracksIntegrationUtil]] is, then probably the
 * frame size is 131072.
 * Note: Project->right click->Open Module Settings->Modules->Dependencies->Provide-Compile
 * Terminal-> sbt -> compile
 */
object Example {
  var sc: SparkContext = null

  val aqlQuery = """
              |
              |    use dataverse feed4;
              |    create type TwitterUser as closed {
              |        screen_name: string,
              |        lang: string,
              |        friends_count: int32,
              |        statuses_count: int32
              |    };
              | create type Tweet as open {
              |        id: int64,
              |        user: TwitterUser
              |    }
              |
              |    create dataset Tweets (Tweet)
              |    primary key id;
              |""".stripMargin

  val sqlppQuery ="""
                    |use dataverse tpcds3;
                    |for $i in dataset inventory return $i;
              | """.stripMargin


  def init() = {
    /**
     * Configure Spark with AsterixDB-Spark connector configurations.
     */
    val conf = new SparkConf()
      .setMaster("local[4]")
      .set("spark.asterix.connection.host", "localhost") //AsterixDB API host
      .set("spark.asterix.connection.port", "19002") //AsterixDB API port
      .set("spark.asterix.frame.size", "32768") //AsterixDB configured frame size
      .set("spark.driver.memory", "1g")
      .setAppName("AsterixDB-Spark Connector Example")

    //Initialize SparkContext with AsterixDB configuration
    sc = new SparkContext(conf)
  }

  /**
   * This example shows how to get AsterixRDD from an AQL query.
   * AsterixRDD usually is not the most useful form as it returns the result
   * as RDD[String]. Until now, AsterixDB does not provide a Java driver. Once
   * that we have it. This form can be useful and less memory intensive.
   */
  def runAsterixRDD() = {
    /* Get AstreixRDD from SparkContext using AQL query.
     * You can use sqlpp() to get the result from running SQL++ query.
     */
      val rddAql = sc.executeJustQuery(aqlQuery, QueryType.AQL) // test for no return query

      println("AQL result")
      // rddAql.collect().foreach(println)
    //show all dataset and dataverse
      val sqlContext= new SQLContext(sc)
      val dataAll=sqlContext.showAll()
      dataAll.show()
    //choose one dataset in a dataverse that you want to use
      val Dataverse="tpcds3";
      val Dataset="inventory"
      val schema=sqlContext.showSchema(Dataverse,Dataset);
      println(schema)
    //use one dataset in a dataverse. Return a Dataframe
      val datasetR=sqlContext.useDataset(Dataverse,Dataset);
      datasetR.show()
     // by the schema of dataframe, we can build table for aql
      val schemaSpark = datasetR.printSchema()
    // print out the schema of a dataframe's attribute one by one
    /*
      (inv_date_sk,LongType,true)
      (inv_item_sk,LongType,true)
      (inv_quantity_on_hand,LongType,true)
      (inv_warehouse_sk,LongType,true)
     */
    val aqlquery = ""
    datasetR.schema.fields.foreach(field => print (field.name, field.dataType.typeName))
    // register (dataframe, dataverse, dataset, key)
    println (sqlContext.createFeed("feeds", "TestDataset"))
    sc.startFeed("feeds", "TestDataset")
    sqlContext.feedFromFileToLocal(  "/Users/MingdaLi/Desktop/ucla_4/spark-asterixDB/asterixdb-spark-connector/chu.adm")
    sc.stopFeed("feeds", "TestDataset")
    // feed data from file to dataverse, dataset

    sc.establishTable(datasetR,"tmpD","tmpT","inv_date_sk, inv_item_sk ,inv_warehouse_sk")
    sqlContext.feedFromDfToFile(datasetR) // build a test.adm file
    //sc.stopFeed("tmpD", "tmpT")
    //sc.startFeed("tmpD", "tmpT")
    // sqlContext.feedFromDfToLocal( datasetR)
    // sqlContext.feedFromDfToFile( datasetR)
    //sqlContext.feedFromFileToLocal(  "/Users/MingdaLi/Desktop/ucla_4/spark-asterixDB/asterixdb-spark-connector/test.txt")

    //sc.stopFeed("tmpD", "tmpT")


  }

  /**
   * This is the best way to interact with AsterixDB using Spark.
   * the query (SQL++ or AQL) result is returned as a DataFrame which
   * can then be used with many of Spark libraries.
   */
  def runAsterixWithDataFrame() = {
    //Create SQLContext from SparkContext
    val sqlContext = new SQLContext(sc)

    /* Get DataFrame by running SQL++ query (AQL also supported by calling aql())
     * infer = true means that we tell AsterixDB to provide Spark the result schema.
     * if that throws an exception, probably you AsterixDB doesn't have the schema inferencer.
     * Therefore, let infer = false and Spark will do the job (with the cost of additional scan).
     */
    val start = System.currentTimeMillis

    val dfSqlpp = sqlContext.aql(sqlppQuery)

    println("SQL++ DataFrame result")
    dfSqlpp.filter(dfSqlpp("inv_item_sk")===38).show()
    val totalTime = System.currentTimeMillis - start
    println("INV time: %1d ms".format(totalTime))


    var start2 = System.currentTimeMillis

    val aQuery ="""
                  |use dataverse feeds;
                  |for $i in dataset Tweets return $i;
                  | """.stripMargin
    var alpp = sqlContext.aql(aQuery)
    alpp.filter(alpp("id")==="861402323485982720").show()
    var totalTime2 = System.currentTimeMillis - start2

    println("TWEET time: %1d ms".format(totalTime2))

     start2 = System.currentTimeMillis

    var bQuery ="""
                  |use dataverse tpcds3;
                  |for $i in dataset catalog_sales return $i;
                  | """.stripMargin
    alpp = sqlContext.aql(bQuery)
    alpp.filter(alpp("cs_sold_date_sk")==="2450816").show()
    var totalTime3 = System.currentTimeMillis - start2

    println("CS time: %1d ms".format(totalTime3))

    bQuery ="""
              |use dataverse tpcds3;
              |for $i in dataset catalog_returns return $i;
              | """.stripMargin
    alpp = sqlContext.aql(bQuery)
    alpp.filter(alpp("cr_returned_date_sk")<"2452057").count()
    var totalTime4 = System.currentTimeMillis - start2

    println("CR time: %1d ms".format(totalTime4))



  }

  /**
   * Run the example.
   * @param args
   */
  def main (args: Array[String]) {
    init()
    runAsterixRDD()
    // runAsterixWithDataFrame()


    sc.stop()
//    val conf = new SparkConf().setAppName("sdfsf").setMaster("local[2]")
//    val sc = new SparkContext(conf)
  }
}
