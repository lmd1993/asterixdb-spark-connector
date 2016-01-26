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
package org.apache.spark.sql.asterix


import org.apache.spark.sql.{DataFrame, SQLContext}


class SQLContextFunctions(@transient sqlContext:SQLContext)
  extends org.apache.spark.Logging with Serializable {

  @transient def admToCaseClass(adm:String) : String = {
    println("/*")
    println(adm)
    println("*/")
    val types = adm.trim.replaceAll("\n","").split('}')
    val res = types.map { t =>
      val classNameFields = t.split('{')

      val className = classNameFields(0)
      val fields = classNameFields(1).split(',').map{ f =>
        val nameType = f.split(':')
        val name = nameType(0).replaceAll(" ", "")

        val typeCamelized = nameType(1)(0).toUpper + nameType(1).substring(1)
        val typeString = typeCamelized match {
          case "Int64" => "Long"
          case list if list.contains('[') => "Array"+list
          case  _ => typeCamelized
        }
        (name, typeString)
      }.sortBy(_._1).map(nt => nt._1 + ":" + nt._2).reduceLeft(_ + ", " + _)


      "case class " + className + "(" + fields + ")\n"
    }.reduceLeft(_+_)
    res
  }

  @transient def aql(query:String, infer:Boolean = false, printCaseClasses:Boolean=false) : DataFrame =
  {
    import org.apache.asterix.connector._

    val sc = sqlContext.sparkContext
    val rdd = sc.aql(query)
    val partitionedRdd = rdd.repartitionAsterix(rdd.getPartitions.length * 2)
    if(infer) {
      log.info("Preparing schema")
      val schemaJSON = rdd.getSchema
      val dummyRdd = sc.parallelize(Seq(schemaJSON.getString("DUMMY_JSON")))
      val dummyDF = sqlContext.read.json(dummyRdd)
      if(printCaseClasses) {
        println("//------------- BEGIN -------------")
        println(admToCaseClass(schemaJSON.getString("ADM")))
        println("//-------------  END  -------------")
      }
       return sqlContext.read.schema(dummyDF.schema).json(partitionedRdd)
    }

    sqlContext.read.json(partitionedRdd)
  }
}