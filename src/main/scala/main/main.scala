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
package main

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.asterix._
import org.apache.asterix.connector._


case class listType1(addr_no:String, dais_id:String, display_name:String, first_name:String, full_name:String, last_name:String, r_id:String, reprint:String, role:String, seq_no:String, suffix:String, wos_standard:String)
case class DatasetType1(count:String, name:Array[listType1])
object main {
  var sc: SparkContext = null

  val aql = """
              |use dataverse twitterDataverse
              |for $x in dataset tweet
              |limit 10
              |return $x
              |""".stripMargin


  def init() = {
    val conf = new SparkConf()
      .setMaster("local[8]")
      .set("spark.asterix.connection.host", "localhost") //AsterixDB API host
      .set("spark.asterix.connection.port", "19002") //AsterixDB API port
      .set("spark.asterix.frameSize", "131072") //AsterixDB compiler frame size see your asterix-build-configuration.xml
      .setAppName("AsterixDB Connector")

    //Initialize SparkContext with AsterixDB configuration
    sc = new SparkContext(conf)


  }

  def testAsterixRDD() = {
    val rdd = sc.aql(aql)
    rdd.collect().foreach(println)
  }

  def testAsterixRDDWithSparkSQL() = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

//    val rdd = sc.aql(aql)

    val df = sqlContext.aql(aql)

//    val df = sqlContext.read.json(rdd)
    df.printSchema()

    df.show(10)


  }

  def main (args: Array[String]): Unit = {
    init()
    testAsterixRDDWithSparkSQL()
  }
}
