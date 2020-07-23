package org.sparkhackathon

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe

object sqlhack {
  def main(args:Array[String]){
    val spark=SparkSession.builder().appName("Spark Part-B").master("local[*]")
    .config("spark.history.fs.logDirectory","file:///tmp/spark-events")
    .config("spark.eventLog.dir","file:///tmp/spark-events")
    .config("spark.eventLog.enabled","true")
    .config("hive.metastore.uris","thrift://localhost:9083")
    .config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
    .enableHiveSupport().getOrCreate();
    spark.sparkContext.setLogLevel("error")

    import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,DateType}
    //Step:20
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

    //Step:21
    //val sparkSession =SparkSession.builder.enableHiveSupport.getOrCreate()
    //val sqlctx=spark.SqlContext

    val df1=spark.read.option("header","true").option("delimiter",",").schema(insureschema).csv("hdfs://localhost:54310/user/hduser/insuranceinfo*.csv")
    df1.printSchema

    //Step:22
    val df2=df1.select(col("IssuerId"),col("IssuerId2"),col("BusinessDate"),col("StateCode").as("stcd"),col("SourceName").as("srcnm"),col("NetworkName"),col("NetworkURL"),col("custnum"),col("MarketCoverage"),col("DentalOnlyPlan"))
    df2.printSchema

    val df3=df2.select(col("IssuerId"),col("IssuerId2"),concat(col("IssuerId"),lit(" "),col("IssuerId2")).as ("issueridcomposite"),col("BusinessDate"),col("stcd"),col("srcnm"),col("NetworkName"),col("NetworkURL"),col("custnum"),col("MarketCoverage"),col("DentalOnlyPlan"))
    df3.printSchema

    val df4=df3.select(col("IssuerId"),col("IssuerId2"),col("issueridcomposite"),col("BusinessDate"),col("stcd"),col("srcnm"),col("NetworkName"),col("NetworkURL"),col("custnum"),col("MarketCoverage"),col("DentalOnlyPlan")).drop("DentalOnlyPlan")
    df4.printSchema

    val df5=df4.withColumn("sysdt",current_date()).withColumn("systs",current_timestamp())
    df5.show(5,false)
    println("Count Before Removal of NULL: "+df5.count)

    //Step:23
    val rmnulldf=df5.na.drop()
    //rmnulldf.count()
    println("Count After Removal of NULL: "+rmnulldf.count)

    //Step:24-25-26
    val reusableobj=new org.inceptez.hack.allmethods
    val udfremsplchars = udf(reusableobj.remspecialchar _)

    val transformeddf=rmnulldf.select(col("IssuerId"),col("IssuerId2"),col("issueridcomposite"),col("BusinessDate"),col("stcd"),col("srcnm"),udfremsplchars(col("NetworkName")) as "NetworkName" ,col("NetworkURL"),col("custnum"),col("MarketCoverage"),col("sysdt"),col("systs"))
    //transformeddf.count()
    transformeddf.show(2,false)
    println("Count After Removing Special Character: "+transformeddf.count)
    //Step:27
    transformeddf.write.mode("overwrite").json("hdfs://localhost:54310/user/hduser/dfjson");
    println("DF saved in HDFS as JSON Format")
    //Step:28
    transformeddf.coalesce(1).write.mode("overwrite").option("header","true").option("delimiter","~").csv("hdfs://localhost:54310/user/hduser/sparkhack2/dfcsv")
    println("DF saved in HDFS as CSV Format")

    //Step:28
    transformeddf.createOrReplaceTempView("dfview")
    spark.sql("drop table if exists custdb.dfhivetbl")
    transformeddf.write.mode("overwrite").saveAsTable("custdb.dfhivetbl")
    println("Table Created in Hive")
    spark.sql("select * from custdb.dfhivetbl").show(3,false)

    //Part:B-Step:30 RDD Functions
    val filerdd = spark.sparkContext.textFile("hdfs://localhost:54310/user/hduser/custs_states.csv")
    println("Loaded Data in RDD Count: "+filerdd.count)
    //Step:31
    val cusfilter=filerdd.map(x=>x.split(",")).filter(p=>p.length==5)
    println("custfilter RDD Count: "+cusfilter.count)
    println(cusfilter.take(3))
    val statesfilter=filerdd.map(x=>x.split(",")).filter(p=>p.length==2)
    println("statesfilter RDD Count: "+statesfilter.count)
    statesfilter.take(3).foreach(println)

    // Using DSL
    // Step:32
    val custstatesdf=spark.read.csv("hdfs://localhost:54310/user/hduser/custs_states.csv")

    //Step: 33
    import spark.implicits._
    val custfilterdf=custstatesdf.filter(($"_c0".isNotNull) and ($"_c1".isNotNull) and ($"_c2".isNotNull) and ($"_c3".isNotNull) and ($"_c4".isNotNull))
    custfilterdf.show(3)
    val statesfilterdf = custstatesdf.filter(($"_c2".isNull) and ($"_c3".isNull) and ($"_c4".isNull))
    statesfilterdf.show(3)

    // Using SQL Queries
    // Step:34
    custfilterdf.createOrReplaceTempView("custview")
    statesfilterdf.createOrReplaceTempView("statesview")
    spark.sql("select * from custview ").show(10)
    spark.sql("select * from statesview ").show(10)

    // Step: 35
    df5.createOrReplaceTempView("insureview")
    spark.catalog.listDatabases.show(10,false);
    spark.catalog.listTables.show(10,false);

    // Step: 36
    import org.apache.spark.sql.functions.{concat,col,lit,udf,max,min,count,avg}
    val reusableobj1=new org.inceptez.hack.allmethods
    spark.udf.register("udfremsplcharsudf",reusableobj1.remspecialcharudf _)
    // Step: 37
    val joindf = spark.sql("""select IssuerId,IssuerId2,issueridcomposite,MONTH(BusinessDate) as mth,YEAR(BusinessDate) as yr,stcd,srcnm,NetworkName,udfremsplcharsudf(NetworkName) as cleannetworkname ,NetworkURL,
                                  (case substring(NetworkURL,0,5) when  'https'  then  'https' else case substring(NetworkURL,0,4) when  'http'  then  'http'  else 'noprotocal' end end) as protocal,custnum,MarketCoverage,
                                  sysdt as curdt,systs as curts,st._c1 as statedesc,cust._c3 as age,cust._c4 as profession
                                  from insureview ins join statesview st on ins.stcd=st._c0 join custview cust on ins.custnum=cust._c0""").toDF
    joindf.show(5,false)

    //Step: 38
    joindf.write.mode("overwrite").option("compression","none").parquet("hdfs://localhost:54310/user/hduser/Parquet")
    println("joindf stored in hdfs as Parquet Format")

    //Step: 39
    joindf.createOrReplaceTempView("joindfview")
    val rowdf=spark.sql("""select row_number() over (partition by protocal order by countage) seq_num,* from
                        (select  avg(age) as Avgage,count(*) as countage,statedesc,protocal,profession
                        from joindfview group by statedesc, protocal, profession order by countage desc) t """)
    rowdf.show(500,false)

    //Step: 40
    println("Writing to mysql")

    val prop=new java.util.Properties();
        prop.put("user", "root")
        prop.put("password", "root")

    rowdf.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/custdb","seqnumdf",prop)
    println("Table created in Mysql")

  }
}
