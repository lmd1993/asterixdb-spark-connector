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

/**
 * AsterixDB-Spark connector configurations.
 */

final class Configuration(val host: String,
                    val port: String,
                    val frameSize: Int,
                    val nFrames: Int,
                    val nReaders: Int,
                    val prefetchThreshold: Int) extends Serializable

object Configuration {

  /**
   * AsterixDB HTTP API host address.
   *
   * Required.
   */
  val AsterixDBHost = "spark.asterix.connection.host"

  /**
   * AsterixDB HTTP API port number.
   *
   * Required.
   */
  val AsterixDBPort = "spark.asterix.connection.port"

  /**
   * AsterixDB compiler frame size.
   * This can be found in your AsterixDB configuration file
   *
   * Required
   */
  val AsterixDBFrameSize = "spark.asterix.frame.size"


  //OPTIONAL Configurations

  /**
   * Number of AsterixDB frames to read at a time.
   * This should NOT be big as the intermediate result can consume large amount memory.
   *
   * Optional.
   * Default: 1
   */
  val AsterixDBFrameNumber = "spark.asterix.frame.number"

  /**
   * The number of parallel readers per AsterixDB result partition.
   *
   * Optional.
   * Default: 2
   */
  val AsterixDBNumberOfReaders = "spark.asterix.reader.number"

  /**
   * The remaining number of unread tuples before trigger the pre-fetcher.
   * This should NOT be big as the intermediate result can consume large amount memory.
   *
   * Optional.
   * Default: 2
   */
  val AsterixDBPrefetchThreshold = "spark.asterix.prefetch.threshold"
}
