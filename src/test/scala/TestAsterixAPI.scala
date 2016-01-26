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

import org.junit.Test

class TestAsterixAPI extends TestFramework{


  @Test
  def testExecuteAsync(): Unit ={

    api.executeAsync(
      """let $x := 'hi'
        |return $x
      """.stripMargin)
  }

  @Test
  def testGetHandle(): Unit =
  {
    val handle = api.executeAsync(
      """let $x := 'hi'
        |return $x
      """.stripMargin)

    val locations = api.getResultLocations(handle)
    println(locations)
  }


  @Test
  def testGetSchema(): Unit ={

    val handle = api.executeAsync(
    """
      |let $x := [
      | {"message":{"m1":"hello","m2":[1,2,3,4]}},
      | {"message":{"m1":"hello","m2":[5,6,7,8]}}
      |]
      |return $x
    """.stripMargin
    )

    println(api.getResultSchema(handle))
  }
}
