package org.sparkhackathon
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

object Streaming_PartC {
  case class chatclass(Id:Int,Chat:String,Type:String)
  def main(args:Array[String])
  {
    
val sparkSession =
SparkSession.builder.appName("textstream")
    .config("hive.metastore.uris","thrift://localhost:9083")
    .config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
    .enableHiveSupport.master("local[*]").getOrCreate();

  val sparkcontext = sparkSession.sparkContext;
  val sqlc =sparkSession.sqlContext
  sparkcontext.setLogLevel("ERROR")

  // Step:1
  val ssc = new StreamingContext(sparkcontext, Seconds(50))
  println("reading streaming file")

  val lines = ssc.textFileStream("file:///home/hduser/sparkdata/streaming")
  lines.cache()
  lines.print()

  // Step:2
  val chatdf=lines.foreachRDD(rdd=>{
  if(!rdd.isEmpty()){
    val chatrdd=rdd.map(x=>x.split("~")).map(x=>chatclass(x(0).toInt,x(1),x(2)))

    // Step:3
    val splitrdd= chatrdd.filter(x=>x.Type=="c")
    splitrdd.foreach {println }
    import sqlc.implicits._
    val df1=chatrdd.toDF()
    df1.show(3)

    // Step:4
    val newdf=df1.drop("Type")
    newdf.createOrReplaceTempView("dfchatview")
    sqlc.sql("select * from dfchatview").show(false)

    // Step:5
    sqlc.sql("select Id,split(Chat,' ') as chat_tokens from dfchatview").show(false)

    // Step:6
    println("creating view chatview")
    val dfchat=sqlc.sql("select Id,explode(split(Chat,' '))as chat_splits from dfchatview").toDF
    dfchat.createOrReplaceTempView("chatview")
    sqlc.sql("select * from chatview").show(false)

    // Step:7
    val stopwords = sparkcontext.textFile("file:////home/hduser/stopwordsdir/stopwords")
    stopwords.foreach(println)
    val stopworddf=stopwords.toDF("stopword")
    stopworddf.createOrReplaceTempView("stopwordview")
    sqlc.sql("select * from stopwordview").show(false)

    // Step:8
    sparkSession.conf.set("spark.sql.crossJoin.enabled", true)
    val joindf=sqlc.sql("select Id,chat_splits from chatview a left outer join stopwordview b on a.chat_splits=b.stopword where chat_splits not in(select stopword from stopwordview ) order by Id").toDF
    joindf.createOrReplaceTempView("joindfview")
    sqlc.sql("select * from joindfview order by Id").show(false)

    // Step: 9
    joindf.write.mode("overwrite").saveAsTable("custdb.joindfhive")
    println("Hive Table Created")

    // Step:10
    sqlc.sql("select chat_splits ,count(*) as occurance from joindfview group by chat_splits having count(*)>1").show(false)
    val joindfviewdf=sqlc.sql("select chat_splits as chatkeywords,count(*) as occurance from joindfview group by chat_splits having count(*)>1").toDF
    joindfviewdf.show(2)

    //step-11
    joindfviewdf.write.mode("overwrite").json("hdfs://localhost:54310/user/hduser/sparkhack2/streamingjson");
    println("joindfviewdf dataframe stored as json in the hdfs")
  }
})
  ssc.start()
  ssc.awaitTermination()
  }
}
