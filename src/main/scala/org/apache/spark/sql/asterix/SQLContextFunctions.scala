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

import org.apache.asterix.connector.QueryType.QueryType
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, DataType, LongType}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.asterix.connector._
import java.io._
import scala.io.Source
import java.net._
/**
 * This class extends SQLContext (implicitly) to query AsterixDB
 * using both aql() and sqlpp() methods.
 *
 * It also adds the ability to let AsterixDB infer the schema without any additional pass.
 *
 * @param sqlContext Spark SQLContext
 */
class SQLContextFunctions(@transient sqlContext:SQLContext)
  extends org.apache.spark.internal.Logging with Serializable {

  private def camelize(value: String): String = {
    value(0) match  {
      case '[' =>
        val arrayType = value.substring(1, value.length - 1).replace("int64", "long")
        "[" + arrayType(0).toUpper + arrayType.substring(1) + "]"

      case _ => value(0).toUpper + value.substring(1)
    }
  }

  @transient
  private def admToCaseClass(adm:String): String = {
    println("/*")
    println(adm)
    println("*/")
    val types = adm.trim.replaceAll("\n", "").split('}')
    val res = types.map { t =>
      val classNameFields = t.split('{')

      val className = camelize(classNameFields(0))
      val fields = classNameFields(1).split(',').map{ f =>
        val nameType = f.split(':')
        val name = nameType(0).replaceAll(" ", "")

        val typeCamelized = camelize(nameType(1))
        val typeString = typeCamelized match {
          case "Int64" => "Long"
          case list if list(0) == '[' => "Array" + list
          case  _ => typeCamelized
        }
        (name, typeString)
      }.sortBy(_._1).map(nt => nt._1 + ": " + nt._2).reduceLeft(_ + ", " + _)


      "case class " + className + "(" + fields + ")\n"
    }.reduceLeft(_ + _)
    res
  }
  /*
   *The method is to return all the dataverse and all the data set information
   */
  @transient
  def showAll(infer:Boolean = false, printCaseClasses:Boolean = false):DataFrame={
    val aqlQuery="""
                   |for $x in dataset Metadata.Dataset return{
                   | "DataverseName":$x.DataverseName,
                   |"DatasetName":$x.DatasetName
                   |};
                   | """.stripMargin
    executeQuery(aqlQuery, QueryType.AQL, infer, printCaseClasses)
  }
  /*
 *The method is to return the dataset's schema
 */
  @transient
  def showSchema(Dataverse:String, Dataset:String, infer:Boolean = false, printCaseClasses:Boolean = false): scala.Unit={
    val aqlQuery=" use dataverse "+ Dataverse+"; for $i in dataset "+Dataset+" limit 2 return $i;"
    executeQuery(aqlQuery, QueryType.AQL, infer, printCaseClasses).printSchema
  }
  /*
  *The method is to return the dataset's schema
  */
  @transient
  def useDataset(Dataverse:String, Dataset:String, infer:Boolean = false, printCaseClasses:Boolean = false): DataFrame={
    val aqlQuery=" use dataverse "+ Dataverse+"; for $i in dataset "+Dataset+" limit 2 return $i;"
    executeQuery(aqlQuery, QueryType.AQL, infer, printCaseClasses)
  }
  def createFeed (dataverse: String, dataset: String, typeName : String = "TestDataType", formatType :String = "adm" ): String= {
    var aqlQuery = " use dataverse " +dataverse +"; "
    aqlQuery = aqlQuery + "drop feed tmpFeed if exists; \n create feed tmpFeed  using socket_adapter ( (\"sockets\"=\"127.0.0.1:10001\"),\n       (\"address-type\"=\"IP\"),\n       (\"type-name\"=\"" +typeName +"\"),     (\"format\"=\""+ formatType+"\"));"
    aqlQuery = aqlQuery + "  connect feed tmpFeed to dataset "+dataset+ ";"
    aqlQuery
  }
  @transient
  def feedFromFileToLocal( filePath: String): Unit ={
    var ip = "127.0.0.1"
    var port1 = 10001
    val ia = InetAddress.getLocalHost()
    val socket = new Socket(ia, 10001)
    val writer = new OutputStreamWriter(socket.getOutputStream());
    for (line <- Source.fromFile(filePath).getLines()) {
      writer.write(line);
      writer.flush();
      writer.close();
    }
    socket.close();
  }
  def convertDfToADM (row:Row, stringL:List[Number], stringC:List[String]) :String ={
    var st = "{"
    var ind = 0
    for {ind <- 0 until row.length}
      if (stringL contains (ind)){
        st = st+"\""+stringC(ind) +"\" :"
        st = st+"\""+row(ind).toString+"\" ,"
      }else{
        st= st+"\""+stringC(ind) +"\" :"
          st = st+row(ind).toString+","
      }
    st = st.dropRight(1)
    st = st + "}"
    println (st)
    st

  }

  def feedFromDfToFile( df: DataFrame) : Unit = {
    var i = 0
    var stringL = List[Number]()
    var stringC = List[String]()
    df.schema.fields.foreach(field => field.dataType match {
      case StringType => {
        stringL = i :: stringL
        i = i + 1
      }
      case _ => i = i + 1
    })
    df.schema.fields.
      foreach(field => stringC = stringC :+ field.name)


    df.foreach(f
    => {
      val fw = new FileWriter("test.adm", true)
      try {
        fw.write( convertDfToADM(f, stringL, stringC))
      }
      finally fw.close()
      /*

      var ip = "127.0.0.1"
      var port1 = 10001
      val ia = InetAddress.getLocalHost()
      val socket = new Socket(ia, 10001)
      val writer = new OutputStreamWriter(socket.getOutputStream());
      writer.write(convertDfToADM(f, stringL, stringC))
      writer.flush()
      writer.close()
      socket.close();*/

    })
  }
  def feedFromDfToLocal( df: DataFrame) : Unit ={
    var i = 0
    var stringL= List[Number]()
    var stringC = List[String]()
    df.schema.fields.foreach(field => field.dataType match {
      case StringType => {
        stringL = i :: stringL
        i = i + 1
      }
      case _ => i = i + 1
    })
    df.schema.fields.
      foreach(field => stringC = stringC :+ field.name)


    df.foreach(f
    =>{
      var ip = "127.0.0.1"
      var port1 = 10001
      val ia = InetAddress.getLocalHost()
      val socket = new Socket(ia, 10001)
      val writer = new OutputStreamWriter(socket.getOutputStream());
      writer.write(convertDfToADM(f, stringL, stringC))
      writer.flush()
      writer.close()
      socket.close();

    })
    /*
    for (line <- Source.fromFile(filePath).getLines()) {
      writer.write(line);
      writer.flush();
      writer.close();
    }
    */

  }


  def typeMap (inputString : DataType): String ={
    val s = inputString match {
      case LongType => "bigint,"
      case _ => "bigint,"
    }
    s
  }
  def partaql (inputDataframe : DataFrame) : String={
    var s = ""
    inputDataframe.schema.fields.foreach(field => s= s+ field.name+": "+typeMap(field.dataType))
    s
  }
  /*
  *The method is to return the dataset's schema
  */
  @transient
  def registerDataFrame(inputDataframe:DataFrame, Dataverse:String, Dataset: String, PrimaryKey: String):String ={

    val aqlQuery=" use dataverse "+ Dataverse+";  create type tmp as closed {" + partaql(inputDataframe).dropRight(1) +
    "} create dataset " +Dataset + "(tmp) primary key " + PrimaryKey +";"

    aqlQuery
  }


  /**
   * The method takes an AQL query and returns a DataFrame.
   * @param aqlQuery AQL query.
   * @param infer By default AsterixDB will NOT provide the schema.
   * @param printCaseClasses This will create case classes that represents the schema.
   * @return
   */
  @transient
  def aql(aqlQuery:String, infer:Boolean = false, printCaseClasses:Boolean = false): DataFrame = {
    executeQuery(aqlQuery, QueryType.AQL, infer, printCaseClasses)
  }

  /**
   * The method takes an AQL query and returns a DataFrame.
   * @param sqlppQuery AQL query.
   * @param infer By default AsterixDB will NOT provide the schema.
   * @param printCaseClasses This will create case classes that represents the schema.
   * @return
   */
  def sqlpp(sqlppQuery: String, infer: Boolean = false, printCaseClasses: Boolean = false): DataFrame = {
    executeQuery(sqlppQuery, QueryType.SQLPP, infer, printCaseClasses)
  }

  @transient
  private def executeQuery(query: String, queryType: QueryType, infer: Boolean,
                           printCaseClasses:Boolean): DataFrame = {
    val sc = sqlContext.sparkContext
    val rdd = queryType match {
      case QueryType.AQL => sc.aql(query)
      case QueryType.SQLPP => sc.sqlpp(query)
    }

    val partitionedRdd = rdd.repartitionAsterix(rdd.getPartitions.length * rdd.configuration.nReaders)

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
      sqlContext.read.schema(dummyDF.schema).json(partitionedRdd)
    }
    else {
      sqlContext.read.json(partitionedRdd)
    }
  }
}
