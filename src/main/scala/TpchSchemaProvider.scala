package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger


import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global


import org.apache.spark.sql.types._

import org.apache.spark.sql._
import org.apache.spark._

// TPC-H table schemas
case class Customer(
  c_custkey: Int,
  c_name: String,
  c_address: String,
  c_nationkey: Int,
  c_phone: String,
  c_acctbal: Double,
  c_mktsegment: String,
  c_comment: String)

case class Lineitem(
  l_orderkey: Int,
  l_partkey: Int,
  l_suppkey: Int,
  l_linenumber: Int,
  l_quantity: Double,
  l_extendedprice: Double,
  l_discount: Double,
  l_tax: Double,
  l_returnflag: String,
  l_linestatus: String,
  l_shipdate: String,
  l_commitdate: String,
  l_receiptdate: String,
  l_shipinstruct: String,
  l_shipmode: String,
  l_comment: String)

case class Nation(
  n_nationkey: Int,
  n_name: String,
  n_regionkey: Int,
  n_comment: String)

case class Order(
  o_orderkey: Int,
  o_custkey: Int,
  o_orderstatus: String,
  o_totalprice: Double,
  o_orderdate: String,
  o_orderpriority: String,
  o_clerk: String,
  o_shippriority: Int,
  o_comment: String)

case class Part(
  p_partkey: Int,
  p_name: String,
  p_mfgr: String,
  p_brand: String,
  p_type: String,
  p_size: Int,
  p_container: String,
  p_retailprice: Double,
  p_comment: String)

case class Partsupp(
  ps_partkey: Int,
  ps_suppkey: Int,
  ps_availqty: Int,
  ps_supplycost: Double,
  ps_comment: String)

case class Region(
  r_regionkey: Int,
  r_name: String,
  r_comment: String)

case class Supplier(
  s_suppkey: Int,
  s_name: String,
  s_address: String,
  s_nationkey: Int,
  s_phone: String,
  s_acctbal: Double,
  s_comment: String)

class TpchSchemaProvider(sc: SparkContext, inputDir: String){

  val log = Logger.getLogger(getClass.getName)

  // this is used to implicitly convert an RDD to a DataFrame.
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._


 log.info("before")

 /*

  val dfMap =  scala.collection.mutable.Map[String, DataFrame]() 
  val fcustomer = Future {
    dfMap("customer")=sc.textFile(inputDir + "/customer").map(_.split('|')).map(p =>
      Customer(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF() 
    0
 }

 val flineitem = Future {
   dfMap("lineitem")= sc.textFile(inputDir + "/lineitem").map(_.split('|')).map(p =>
      Lineitem(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF() 

   0
}

 val fnation = Future {
   dfMap("nation") = sc.textFile(inputDir + "/nation").map(_.split('|')).map(p =>Nation(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim)).toDF()
  0
 }

 val fregion = Future{
   dfMap("region")=  sc.textFile(inputDir + "/region").map(_.split('|')).map(p =>Region(p(0).trim.toInt, p(1).trim, p(2).trim)).toDF()
   0
  }

 val forder = Future{
   dfMap("order") = sc.textFile(inputDir + "/orders").map(_.split('|')).map(p =>Order(p(0).trim.toInt, p(1).trim.toInt, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toInt, p(8).trim)).toDF()
   0
  }

  val fpart = Future{
   dfMap("part") = sc.textFile(inputDir + "/part").map(_.split('|')).map(p =>Part(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toInt, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF()
  0 
  }

  val fpartsupp = Future{
   dfMap("partsupp")= sc.textFile(inputDir + "/partsupp").map(_.split('|')).map(p =>Partsupp(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble, p(4).trim)).toDF()
   0
  } 


  val fsupplier = Future{
   dfMap("supplier") =sc.textFile(inputDir + "/supplier").map(_.split('|')).map(p =>Supplier(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()
   0
  }


  Await.result(fsupplier,10 second)
  Await.result(fpartsupp,10 second)
  Await.result(fpart,10 second)
  Await.result(forder,10 second)

  Await.result(fregion,10 second)
  Await.result(fnation,10 second)
  Await.result(flineitem,10 second)
  Await.result(fcustomer,10 second)


  */


 
  val dfMap = Map(
    "customer" -> sc.textFile(inputDir + "/customer").map(_.split('|')).map(p =>
     Customer(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF(),

    "lineitem" -> sc.textFile(inputDir + "/lineitem").map(_.split('|')).map(p =>
      Lineitem(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF(),

    "nation" -> sc.textFile(inputDir + "/nation").map(_.split('|')).map(p =>
      Nation(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim)).toDF(),

    "region" -> sc.textFile(inputDir + "/region").map(_.split('|')).map(p =>
      Region(p(0).trim.toInt, p(1).trim, p(2).trim)).toDF(),

    "order" -> sc.textFile(inputDir + "/orders").map(_.split('|')).map(p =>
      Order(p(0).trim.toInt, p(1).trim.toInt, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toInt, p(8).trim)).toDF(),

    "part" -> sc.textFile(inputDir + "/part").map(_.split('|')).map(p =>
      Part(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toInt, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF(),

    "partsupp" -> sc.textFile(inputDir + "/partsupp").map(_.split('|')).map(p =>
      Partsupp(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble, p(4).trim)).toDF(),

    "supplier" -> sc.textFile(inputDir + "/supplier").map(_.split('|')).map(p =>
      Supplier(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF(),
  
    "customer2" -> sc.textFile(inputDir + "/customer").map(_.split('|')).map(p =>
     Customer(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF(),

    "lineitem2" -> sc.textFile(inputDir + "/lineitem").map(_.split('|')).map(p =>
      Lineitem(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF(),

    "nation2" -> sc.textFile(inputDir + "/nation").map(_.split('|')).map(p =>
      Nation(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim)).toDF(),

    "region2" -> sc.textFile(inputDir + "/region").map(_.split('|')).map(p =>
      Region(p(0).trim.toInt, p(1).trim, p(2).trim)).toDF(),

    "order2" -> sc.textFile(inputDir + "/orders").map(_.split('|')).map(p =>
      Order(p(0).trim.toInt, p(1).trim.toInt, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toInt, p(8).trim)).toDF(),

    "part2" -> sc.textFile(inputDir + "/part").map(_.split('|')).map(p =>
      Part(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toInt, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF(),

    "partsupp2" -> sc.textFile(inputDir + "/partsupp").map(_.split('|')).map(p =>
      Partsupp(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble, p(4).trim)).toDF(),

    "supplier2" -> sc.textFile(inputDir + "/supplier").map(_.split('|')).map(p =>
      Supplier(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF(),
  
    "customer3" -> sc.textFile(inputDir + "/customer").map(_.split('|')).map(p =>
     Customer(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF(),

    "lineitem3" -> sc.textFile(inputDir + "/lineitem").map(_.split('|')).map(p =>
      Lineitem(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF(),

    "nation3" -> sc.textFile(inputDir + "/nation").map(_.split('|')).map(p =>
      Nation(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim)).toDF(),

    "region3" -> sc.textFile(inputDir + "/region").map(_.split('|')).map(p =>
      Region(p(0).trim.toInt, p(1).trim, p(2).trim)).toDF(),

    "order3" -> sc.textFile(inputDir + "/orders").map(_.split('|')).map(p =>
      Order(p(0).trim.toInt, p(1).trim.toInt, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toInt, p(8).trim)).toDF(),

    "part3" -> sc.textFile(inputDir + "/part").map(_.split('|')).map(p =>
      Part(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toInt, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF(),

    "partsupp3" -> sc.textFile(inputDir + "/partsupp").map(_.split('|')).map(p =>
      Partsupp(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble, p(4).trim)).toDF(),

    "supplier3" -> sc.textFile(inputDir + "/supplier").map(_.split('|')).map(p =>
      Supplier(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()
    )
  


  log.info("after")
  // for implicits
  val customer = dfMap.get("customer").get
  val lineitem = dfMap.get("lineitem").get
  val nation = dfMap.get("nation").get
  val region = dfMap.get("region").get
  val order = dfMap.get("order").get
  val part = dfMap.get("part").get
  val partsupp = dfMap.get("partsupp").get
  val supplier = dfMap.get("supplier").get

  dfMap.foreach {
    case (key, value) => value.createOrReplaceTempView(key)
  }
}
