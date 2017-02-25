package org.apache.asterix.connector.example
import org.apache.asterix.connector.example.TPCSchemas.{CatalogReturns, CatalogSales, Inventory}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}/**
 * Created by MingdaLi on 1/4/17.
 */
object Baseline_SparkSQL {
  def join_with_Dataframe(sc: SparkContext, sqlContext: SQLContext, _scale:String, _partitions:Int, catalog_returns_path: String, catalog_sales_path: String, inventory_path:String, statement:String, explain: Boolean): Unit = {
    import sqlContext.implicits._

    val catalog_returns = sc.textFile(catalog_returns_path, _partitions).map(line => line.substring(0, line.length-1).split("\\|")).map(x => CatalogReturns(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16))).toDF()
    val catalog_sales = sc.textFile(catalog_sales_path, _partitions).map(line => line.substring(0, line.length-1).split("\\|")).map(x => CatalogSales(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17),x(18),x(19))).toDF()
    val inventory = sc.textFile(inventory_path, _partitions).map(line => line.substring(0, line.length-1).split("\\|")).map(x => Inventory(x(0),x(1), x(2),if(x.size < 4) "null" else x(3))).toDF()

    catalog_returns.registerTempTable("CATALOGRETURNS")
    catalog_sales.registerTempTable("CATALOGSALES")
    inventory.registerTempTable("INVENTORY")

    val join_statement = sqlContext.sql(statement)
    // join_statement.explain(explain)
    println(join_statement.count())
  }

  def main (args: Array[String]) {
    val scale_factor = "1"
    val numPartitions =10
    val _type =0
    val path = "file:/Users/MingdaLi/Desktop/ucla_4/vm-shared/data/"
    //val path = ""
    var catalog_returns_path = path + "catalog_returns"
    var catalog_sales_path = path + "catalog_sales"
    var inventory_path = path +"inventory"
    if (scale_factor == "1"){
      catalog_returns_path= catalog_returns_path + ".dat"
      catalog_sales_path = catalog_sales_path + ".dat"
      inventory_path =inventory_path + ".dat"
    }

    val sparkConf = new SparkConf().setAppName("Join_SparkSQL").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val join_statement = if (_type == 1) {
      //wrong order
      "SELECT cr_returned_date_sk, cr_return_time_sk, cr_item_sk, cr_refunded_customer_sk, cs_sold_date_sk, cs_sold_time_sk, cs_item_sk, cs_order_number, inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, inv_item_sk FROM INVENTORY JOIN CATALOGSALES ON inv_item_sk=cs_item_sk JOIN CATALOGRETURNS ON cs_item_sk=cr_item_sk AND cs_order_number=cr_order_number"
    } else {
      //right order
      "SELECT cr_returned_date_sk, cr_return_time_sk, cr_item_sk, cr_refunded_customer_sk, cs_sold_date_sk, cs_sold_time_sk, cs_item_sk, cs_order_number, inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, inv_item_sk FROM CATALOGRETURNS JOIN CATALOGSALES ON cs_item_sk=cr_item_sk AND cs_order_number=cr_order_number JOIN INVENTORY ON inv_item_sk=cs_item_sk"
    }

    val start_T = System.currentTimeMillis()
    join_with_Dataframe(sc, sqlContext, scale_factor,numPartitions,catalog_returns_path, catalog_sales_path, inventory_path, join_statement, false)
    val end_T = System.currentTimeMillis()
    println("time:"+(end_T-start_T))
  }
}
