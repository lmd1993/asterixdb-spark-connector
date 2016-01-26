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
import org.apache.spark.sql.SQLContext
import org.junit.Test
import org.apache.asterix.connector._
import org.apache.spark.sql.asterix._

class TestAsterixRDD extends TestFramework{

  val aql = """
              |use dataverse wosDataverse
              |for $x in dataset wos
              |let $summary := $x.static_data.summary
              |let $names := $summary.names
              |where $names.count != "1"
              |return $names
            """.stripMargin
  @Test
  def testAsterixRDD() = {
    val rdd = sc.aql(aql)
    rdd.collect().foreach(println)

  }

  /*
   *
   */
  @Test
  def testAsterixRDDWithSparkSQL() = {
    val aqlContext = new SQLContext(sc)
    val df = aqlContext.aql(aql)
    df.registerTempTable("twitter")


    df.printSchema()
    println(df)
    df.select("name.display_name").show()

  }

  @Test
  def testSparkWithDeepJsonFile() = {
    val sqlContext = new SQLContext(sc)
    sqlContext.read.json("/Users/wail/Dropbox (MIT)/AsterixConnector/jsonExample copy.json").registerTempTable("json")


  }
}
