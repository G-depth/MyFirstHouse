package day1112

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, types}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * 统计当天销售额 ，若没有 ，则需要过滤
  */
object Demosales {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("demo").setMaster("local")
    val sc = new SparkContext(conf)

    val sQLContext = new SQLContext(sc)

    val array=Array(

      "2019-11-12,55.5,1122",
      "2019-11-11,88.5,1133",
      "2019-11-11,70.5,1144",
      "2019-11-12,60.5,1155",
      "2019-11-12,55.5,1166",
      "2019-11-11,55.6,1177",
      "2019-11-12,1188",
      "2019-11-11,1199",
      "2019-11-12,55.5,1122"
    )

    val rdd = sc.makeRDD(array)
  val rdd1: RDD[String] = rdd.filter(x => if(x.split(",").length==3)true else false)

    val rows: RDD[Row] = rdd1.map(x => {
      val sth = x.split(",")

      Row(x.split(",")(0),x.split(",")(1),x.split(",")(2).toDouble)

    })
    val structType = types.StructType(
      Array(
        StructField("time ",StringType) ,
        StructField ("salesmoney",DoubleType),
        StructField("id",DoubleType)
      )
    )


    val df: DataFrame = sQLContext.createDataFrame(rows,structType)

    df.createTempView("zzc")
    df.show()

//    sQLContext.sql("select time, sum(salesmoney )  from zzc group by time ").show()

  }



}
