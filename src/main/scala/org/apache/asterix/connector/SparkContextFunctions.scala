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
package org.apache.asterix.connector

import org.apache.asterix.connector.QueryType.QueryType
import org.apache.asterix.connector.rdd.AsterixRDD
import org.apache.asterix.connector.result.AsterixClient
import org.apache.hyracks.api.dataset.DatasetDirectoryRecord.Status
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.asterix.SQLContextFunctions
import org.apache.spark.sql.types.{StringType, LongType, DataType}

import scala.util.{Failure, Success, Try}

/**
 * This class extends SparkContext (implicitly) to query AsterixDB.
 * @param sc SparkContext
 */
class SparkContextFunctions(@transient sc: SparkContext) extends Serializable with org.apache.spark.internal.Logging{

  private val WaitTime = 100;
  private val configuration: Configuration = {
    val sparkConf = sc.getConf

    //Non-optional configurations
    val host: String = sparkConf.get(Configuration.AsterixDBHost)
    val port: String = sparkConf.get(Configuration.AsterixDBPort)
    val frameSize: String = sparkConf.get(Configuration.AsterixDBFrameSize)

    //Optional configurations
    val nFrame: Int = Try(sparkConf.get(Configuration.AsterixDBFrameNumber)) match {
      case Success(n) => n.toInt
      case Failure(e) => AsterixClient.NUM_FRAMES
    }

    val nReader: Int = Try(sparkConf.get(Configuration.AsterixDBNumberOfReaders)) match {
      case Success(n) => n.toInt
      case Failure(e) => AsterixClient.NUM_READERS
    }

    val prefetchThreshold: Int = Try(sparkConf.get(Configuration.AsterixDBPrefetchThreshold)) match {
      case Success(n) => n.toInt
      case Failure(e) => AsterixClient.PREFETCH_THRESHOLD
    }

    logInfo(Configuration.AsterixDBHost + " " + host)
    logInfo(Configuration.AsterixDBPort + " " + port)
    logInfo(Configuration.AsterixDBFrameSize + " " + frameSize)
    logInfo(Configuration.AsterixDBFrameNumber + " " + nFrame)
    logInfo(Configuration.AsterixDBNumberOfReaders + " " + nReader)
    logInfo(Configuration.AsterixDBPrefetchThreshold + " " + prefetchThreshold)

    new Configuration(
      host,
      port,
      frameSize.toInt,
      nFrame,
      nReader,
      prefetchThreshold
    )
  }

  private val api = new AsterixHttpAPI(configuration)

  def aql(aql:String): AsterixRDD = {
    executeQuery(aql, QueryType.AQL)
  }
  def sqlpp(sqlpp:String): AsterixRDD = {
    executeQuery(sqlpp, QueryType.SQLPP)
  }
  // without return value: for the register table, create feed and etc.
  def executeJustQuery(query: String, queryType: QueryType = QueryType.AQL): Any = {
    val handle = queryType match {
      case QueryType.AQL => api.executeJustAQL(query)
      case QueryType.SQLPP => api.executeSQLPP(query)
    }
  }
  def startFeed(dataverse: String, dataset: String, typeName : String = "tmpType", formatType :String = "adm"): Any = {
    var aqlQuery = " use dataverse " +dataverse +"; "
    aqlQuery = aqlQuery + "drop feed tmpFeed if exists; \n create feed tmpFeed  using socket_adapter ( (\"sockets\"=\"127.0.0.1:10001\"),\n       (\"address-type\"=\"IP\"),\n       (\"type-name\"=\"" +typeName +"\"),     (\"format\"=\""+ formatType+"\"));"
    aqlQuery = aqlQuery + "  connect feed tmpFeed to dataset "+dataset+ ";"
    print(aqlQuery)
    val handle = api.executeJustAQL(aqlQuery)
  }


  def typeMap (inputString : DataType): String ={
    val s = inputString match {
      case LongType => "int32,"
      case StringType => "string,"
      case _ => "bigint,"
    }
    s
  }
  def partaql (inputDataframe : DataFrame) : String={
    var s = ""
    inputDataframe.schema.fields.foreach(field => s= s+ field.name+": "+typeMap(field.dataType))
    s
  }
  def establishTable(inputDataframe:DataFrame, Dataverse:String, Dataset: String, PrimaryKey: String): Any = {
    val aqlQuery=" create dataverse "+ Dataverse+  " if not exists; use dataverse "+ Dataverse+";  create type tmpType as open {" + partaql(inputDataframe).dropRight(1) +
      "}; create dataset " +Dataset + "(tmpType) primary key " + PrimaryKey +";"

    println(aqlQuery)
    val handle = api.executeJustAQL(aqlQuery)
  }


  def stopFeed(dataverse: String, dataset: String):Any ={
    api.executeJustAQL("use dataverse " + dataverse +"; disconnect tmpFeed from dataset "+ dataset +";")
  }

  private def executeQuery(query: String, queryType: QueryType): AsterixRDD = {
    val handle = queryType match {
      case QueryType.AQL => api.executeAQL(query)
      case QueryType.SQLPP => api.executeSQLPP(query)
    }
    var isRunning = true

    while(isRunning) {
      val status = api.getStatus(handle)
      status match {
        case Status.SUCCESS => isRunning = false
        case Status.FAILED => throw new AsterixConnectorException("Job " + handle.jobId + " failed.")
        case _ => wait(WaitTime) //Status.RUNNING
      }

    }
    val resultLocations = api.getResultLocations(handle)
    val rdd = new AsterixRDD(sc, query, api, resultLocations, handle, configuration)
    rdd
  }

}
