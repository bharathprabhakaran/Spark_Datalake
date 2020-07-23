package org.sparkhackathon
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Date

object shack {

  case class insureclass (IssuerId:Int,IssuerId2:Int,BusinessDate:Date,StateCode:String,SourceName:String,NetworkName:String,NetworkURL:String,custnum:String,MarketCoverage:String,DentalOnlyPlan:String)

  def main (args:Array[String])
  {
    // Part-A 1.Data cleaning, cleansing, scrubbing
    val conf = new SparkConf().setAppName("Sparkcore").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)
    sc.setLogLevel("ERROR");
    val spark=SparkSession.builder().appName("spark part-A").master("local[*]")
    .config("spark.history.fs.logDirectory","file:///tmp/spark-events")
    .config("spark.eventLog.dir","file:///tmp/spark-events")
    .getOrCreate();


    // Step: 1
    println("Loading Insuranceinfo1")
    val hadooprdd= sc.textFile("hdfs://localhost:54310/user/hduser/insuranceinfo1.csv")

    // Step: 2
    val header=hadooprdd.first()
    val rddrmhead=hadooprdd.filter(x=>x!=header)

    // Step: 3
    println("Count with Header: "+hadooprdd.count)
    println("Count without Header: "+rddrmhead.count)
    rddrmhead.take(5).foreach(println)

    // Step: 4 (Some Doubt on this Step)
    // val rddrmhead1=rddrmhead.filter(x=> !x.isEmpty)
    val rddrmhead1=rddrmhead.filter(x=>x.length!=0)
    //println("Count after Removing Blank Line: "+rddrmhead1.count)

    // Step: 5
    val rddsplit=rddrmhead1.map(x=>x.split(",",-1))
    println(rddsplit.first())

    // Step: 6
    val fieldrdd=rddsplit.filter(x=>x.length==10)
    //println("count no.of records with 10 colums: "+fieldrdd.count)

    // Step: 7
    val reusabledate=new org.inceptez.hack.allmethods
    val schemardd=fieldrdd.map(x=>insureclass(x(0).toInt,x(1).toInt,Date.valueOf(x(2)),x(3),x(4),x(5),x(6),x(7),x(8),x(9)))

    //Step: 8
    println("count of RDD with header data inital load: "+hadooprdd.count)
    println("count of RDD with data split and filter using length: "+rddrmhead1.count)
    println("count of RDD after cleanup: "+schemardd.count)

    //Step: 9
    val rejectdata=rddsplit.filter(x=>x.length!=10)
    println("Count of Rejected Data: "+rejectdata.count)
    println(rejectdata.collect)

    // Step: 10 & 11
    println("Loading Insuranceinfo2")
    val hadooprdd2= sc.textFile("hdfs://localhost:54310/user/hduser/insuranceinfo2.csv")
    val header2=hadooprdd2.first()
    val rddrmhead2=hadooprdd2.filter(x=>x!=header2)
    //having doubt in the below code for point-4 in document
    //val rddrmhead3=rddrmhead2.filter(x=> !x.isEmpty)
    val rddrmhead3=rddrmhead2.filter(x=>x.length!=0)
    val rddsplit2=rddrmhead3.map(x=>x.split(",",-1))
    rddsplit2.first()
    val fieldrdd2=rddsplit2.filter(x=>x(0)!="")
    val schemardd2=fieldrdd2.map(x=>insureclass(x(0).toInt,x(1).toInt,Date.valueOf(reusabledate.dateformat(x(2))),x(3),x(4),x(5),x(6),x(7),x(8),x(9)))
    println("count of RDD with header data inital load: "+hadooprdd2.count)
    println("count of RDD with data split and filter using length: "+rddrmhead2.count)
    println("count of RDD after cleanup: "+schemardd2.count)

    val rejectdata2=rddsplit2.filter(x=>x(0)=="")
    println("Count of Rejected Data: "+rejectdata2.count)
    println(rejectdata2.collect)

    // Step: 12 Data merging, Deduplication, Performance Tuning & Persistance
    println("Merging two rdd's")
    val insuredatamerged=schemardd.union(schemardd2)
    //val merge=fieldrdd.union(fieldrdd2)
    // Step: 13
    insuredatamerged.cache

    // Step: 14
    println("Count of step7: "+schemardd.count)
    println("Count of step11: "+schemardd2.count)
    println("Count of Merged Data: "+insuredatamerged.count)

    // Step: 15
    val rmduplicaterdd=insuredatamerged.distinct()
    println("Count after Duplicat Removed: "+rmduplicaterdd.count)
    val duplicaterdd=insuredatamerged.count-rmduplicaterdd.count
    println("Count of Duplicate Records: "+duplicaterdd)

    // Step: 16
    val insuredatarepart=insuredatamerged.repartition(8)
    //val insuredatarepart1=merge.repartition(8)
    println("Partition Size: "+insuredatarepart.partitions.size)
    insuredatarepart.glom().collect

    // Step: 17
    val rdd_20191001=insuredatarepart.filter(x=> (x.BusinessDate.toString=="2019-10-01"))
    println("Count of BusinessDate 01102019: "+rdd_20191001.count)
    val rdd_20191002=insuredatarepart.filter(x=> (x.BusinessDate.toString=="2019-10-02"))
    println("Count of BusinessDate 02102019: "+rdd_20191002.count)

    // Step: 18
   /* rejectdata.saveAsTextFile("hdfs://localhost:54310/user/hduser/rejectdata")
    println("Rejaected Data from Step:9 stord in HDFS")
    insuredatamerged.saveAsTextFile("hdfs://localhost:54310/user/hduser/insuredatamerged")
    println("Merged data from Step:12 stored in HDFS")
    rdd_20191001.saveAsTextFile("hdfs://localhost:54310/user/hduser/rdd_20191001")
    rdd_20191002.saveAsTextFile("hdfs://localhost:54310/user/hduser/rdd_20191002")
    println("Split Data from Step:17 stord in HDFS")*/

    // Step: 19

    //import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,DateType}
    import sqlc.implicits._

    val insureschema= StructType(Array(StructField("IssuerId",IntegerType,true),
    StructField("IssuerId2",IntegerType,true),
    StructField("BusinessDate",DateType,true),
    StructField("StateCode",StringType,true),
    StructField("SourceName",StringType,true),
    StructField("NetworkName",StringType,true),
    StructField("NetworkURL",StringType,true),
    StructField("custnum",StringType,true),
    StructField("MarketCoverage",StringType,true),
    StructField("DentalOnlyPlan",StringType,true)))

    val df= insuredatarepart.coalesce(1).toDF
    val insuredatarepartdf=sqlc.createDataFrame(df.rdd,insureschema)
    println("Dataframe Created from Merged Data")
    insuredatarepartdf.show(5,false)

  }
}
