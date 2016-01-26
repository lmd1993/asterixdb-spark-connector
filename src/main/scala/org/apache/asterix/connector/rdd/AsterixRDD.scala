package org.apache.asterix.connector.rdd



import org.apache.asterix.connector.result.{AsterixResultReader, ResultUtils}
import org.apache.asterix.connector.{AddressPortPair, Handle, ResultLocations, AsterixAPI}
import org.apache.asterix.result.ResultReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD
import org.json.JSONObject
import scala.collection.JavaConversions._


/**
 * Created by wail on 9/30/15.
 */
class AsterixRDD(@transient sc: SparkContext,
                 @transient val aql:String,
                 @transient val api: AsterixAPI,
                 @transient val locations: ResultLocations,
                 @transient val handle: Handle,
                 @transient val pushToAsterix: Boolean = false)
  extends RDD[String](sc, Seq.empty){

  private var nReaders: Int = ResultReader.NUM_READERS

  override def getPreferredLocations(split:Partition) : Seq[String] = {
    val location = split.asInstanceOf[AsterixPartition].location.address
    Seq(location)
  }

//  private def getLocations : ResultLocations = {
//    api.getResultLocations(handle)
//  }


  override def getPartitions : Array[Partition] = {
    val resultLocations = locations
    val distinctLocations = resultLocations.locations.zipWithIndex

    val part = distinctLocations.map(x=> AsterixPartition(x._2,resultLocations.handle,x._1))
    part.toArray
  }

  @transient def limit(num: Int) : String  = {
    val lastIndexOfReturn = aql.lastIndexOf("return")
    val newAql = aql.substring(0,lastIndexOfReturn) +
                 s"""
                   |limit $num
                   |${aql.substring(lastIndexOfReturn)}
                 """.stripMargin

    newAql
  }

  @transient def repartitionAsterix(numPartitions: Int): RDD[String] ={
    val count = getPartitions.length
    nReaders = Math.ceil(numPartitions/count).asInstanceOf[Int]
    super.repartition(numPartitions)
  }

  @transient def aqlCount(): String ={
    val newLineIndex = aql.indexOf(";")
    val useDataverse =aql.substring(0,newLineIndex)
    val query = aql.substring(newLineIndex+1)
    val newAql = useDataverse + "\n let $sparkVarCount := (" + query +") return count($sparkVarCount)"

    println(newAql)

    newAql
  }

  @transient def getSchema  : JSONObject = {
    api.getResultSchema(handle)
  }

  override def compute(split:Partition, context:TaskContext): Iterator[String] = {
    val partition = split.asInstanceOf[AsterixPartition]
    val resultReader = new AsterixResultReader(partition.location, partition.index, partition.handle, nReaders)
    val results = new ResultUtils
    val startTime = System.nanoTime()
    

    context.addTaskCompletionListener{(context) =>
      val endTime = System.nanoTime()
      logInfo("Finish from running partition:" + partition.index + " in " + ((endTime-startTime)/1000000000d) + "s")
    }


    results.displayResults(resultReader).iterator()
  }

}
