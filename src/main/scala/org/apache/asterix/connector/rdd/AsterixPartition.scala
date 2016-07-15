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
package org.apache.asterix.connector.rdd

import org.apache.asterix.connector.{Handle, AddressPortPair}
import org.apache.spark.Partition

trait LocationPartition extends Partition{
}

/**
 * AsterixDB result partition information holder.
 * Result partition is identified by (index, address, port).
 *
 * @param index partition number
 * @param handle AsterixDB query result handle
 * @param location The address and the port of the result in Hyracks cluster.
 */
case class AsterixPartition(index: Int,
                            handle: Handle,
                            location:AddressPortPair) extends LocationPartition
