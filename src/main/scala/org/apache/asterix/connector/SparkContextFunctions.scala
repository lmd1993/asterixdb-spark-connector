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

import org.apache.asterix.connector.rdd.AsterixRDD
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

class SparkContextFunctions(@transient sc: SparkContext) extends Serializable{

  private val api = new AsterixAPI(sc)

  private def query(aql:String) :Handle = {
    api.executeAsync(aql)
  }

  private def getLocations(handle: Handle) :ResultLocations = {

    val locations = api.getResultLocations(handle)
    locations
  }


  def aql(aql:String): AsterixRDD = {
    val handle = query(aql)
    val resultLocations = getLocations(handle)
    val rdd = new AsterixRDD(sc, aql, api, resultLocations, handle)
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    rdd
  }

}
