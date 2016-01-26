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


import java.io.InputStream
import java.net.URLEncoder


import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost, HttpGet}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.hyracks.api.dataset.ResultSetId
import org.apache.hyracks.api.job.JobId
import org.apache.spark.SparkContext
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import net.liftweb.json.Serialization.read
import org.json.JSONObject


case class Handle(jobId: JobId, resultSetId: ResultSetId)
case class AddressPortPair(address: String, port: String)
case class ResultLocations(handle:Handle, locations:Seq[AddressPortPair])

//For JSON serialization purposes
private[this] case class LocationsJson(locations:Seq[AddressPortPair])
private[this] case class HandleJson(handle: Seq[Long])

class AsterixAPI(@transient sc: SparkContext) extends org.apache.spark.Logging {

  private def host = sc.getConf.get("spark.asterix.connection.host")
  private def port = sc.getConf.get("spark.asterix.connection.port")
  private def apiURL = s"http://$host:$port/"


  private def getHandleJson(handle: Handle) : HandleJson = HandleJson(Seq[Long](handle.jobId.getId,
    handle.resultSetId.getId))

  implicit val formats = DefaultFormats

  def connect(): Boolean = {

    val query =
      """let $x := 'Hello World'
        | return $x
      """.stripMargin

    log.info(s"Connecting to AsterixDB on http://$host:$port")
    val result = execute(query)
    result.contains("Hello World")
  }

  def executeAsync(query: String): Handle =
  {
    val jsonString = execute(query, async = true)

    jsonString.contains("error-code") match {
      case true =>
        val startOffset = jsonString.indexOf(",")
        val endOffset = jsonString.indexOf("]")
        throw new AsterixConnectorException(jsonString.substring(startOffset+1,endOffset));
      case false =>
    }

    val handleJson = read[HandleJson](jsonString)
    val jobId = new JobId(handleJson.handle.head)
    val resultSetId = new ResultSetId(handleJson.handle(1))
    val handle = Handle(jobId, resultSetId)
    log.info(handle.toString)
    handle
  }

  def getStatus(handle: Handle) : String = {
    val handleJSON = getHandleJson(handle: Handle)
    val handleJSONString = URLEncoder.encode(write(handleJSON),"UTF-8")

    log.debug("Get status of: " + handleJSONString)

    val url = apiURL + "query/status?handle=" + handleJSONString

    val response = GETRequest(url)

    response
  }

  def getResultSchema(handle: Handle) : JSONObject = {
    val httpclient: CloseableHttpClient = HttpClients.createDefault

    val handleJSON = getHandleJson(handle: Handle)
    val handleJSONString = URLEncoder.encode(write(handleJSON),"UTF-8")

    log.debug("Get schema of: " + handleJSONString)

    val url = apiURL+"query/schema?handle=" + handleJSONString + "&schema-format=ADM_AND_DUMMY_JSON"
    val response = GETRequest(url)
    new JSONObject(response)
  }


  def getResultLocations(handle: Handle) : ResultLocations = {

    val handleJSON = getHandleJson(handle: Handle)
    val handleJSONString = URLEncoder.encode(write(handleJSON),"UTF-8")

    log.debug("Get locations of: " + handleJSONString)

    val url = apiURL + "query/locations?handle=" + handleJSONString

    val response = GETRequest(url)
    log.info("Result Locations: " + response)
    val locations = read[LocationsJson](response)
    ResultLocations(handle, locations.locations)
  }

  private def execute(query :String, async:Boolean = false) : String= {
    val url = async match {
      case true => apiURL+"aql?mode=asynchronous&schema-inferencer=Spark"
      case false => apiURL+"aql"
    }
    val response = POSTRequest(url,query)

    if(async)
      log.info("Response Handle: " + response)
    else
      log.debug("Response: " + response)

    response
  }

  private def GETRequest(url: String) : String = {
    val httpclient: CloseableHttpClient = HttpClients.createDefault
    val httpGet: HttpGet = new HttpGet(url)
    val response: CloseableHttpResponse = httpclient.execute(httpGet)
    val responseContent: InputStream = response.getEntity.getContent
    val responseString = scala.io.Source.fromInputStream(responseContent).mkString
    httpclient.close()
    responseString
  }

  private def POSTRequest(url: String , entity:String) : String = {
    val httpclient: CloseableHttpClient = HttpClients.createDefault
    val httpPost: HttpPost = new HttpPost(url)
    httpPost.setEntity(new StringEntity(entity))
    val response: CloseableHttpResponse = httpclient.execute(httpPost)
    val responseContent: InputStream = response.getEntity.getContent
    val responseString = scala.io.Source.fromInputStream(responseContent).mkString
    response.close()
    httpclient.close()
    responseString
  }


}
