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
 *
 */
object Example {
  var sc: SparkContext = null

  val aqlQuery = """
              |let $exampleSet := [
              | {"name" : "Ann", "age" : 20, "salary" : 100000},
              | {"name" : "Bob", "age" : 30, "salary" : 200000},
              | {"name" : "Cat", "age" : 40, "salary" : 300000, "dependencies" : [1, 2, 3]}
              |]
              |for $x in $exampleSet
              |return $x
              |""".stripMargin

  val sqlppQuery ="""
              | SELECT element exampleSet
              | FROM [
              | {"name" : "Ann", "age" : 20, "salary" : 100000},
              | {"name" : "Bob", "age" : 30, "salary" : 200000},
              | {"name" : "Cat", "age" : 40, "salary" : 300000, "dependencies" : [1, 2, 3]}
              | ] as exampleSet;
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

    val rddAql = sc.aql(aqlQuery)

    println("AQL result")
    rddAql.collect().foreach(println)
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
    val dfSqlpp = sqlContext.sqlpp(sqlppQuery, infer = true)

    println("SQL++ DataFrame result")
    dfSqlpp.printSchema()
    dfSqlpp.show()

  }

  /**
   * Run the example.
   * @param args
   */
  def main (args: Array[String]): Unit = {
    init()
    runAsterixRDD()
    runAsterixWithDataFrame()
  }
}
