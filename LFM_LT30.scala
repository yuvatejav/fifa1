import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame


object LFM_LT30 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder().appName("LFM_LT30").master("local[*]")
      .config("hive.stats.dbclass","jdbc:mysql")
      //.config("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
      //.config("javax.jdo.option.ConnectionURL","jdbc:mysql://localhost:3306/metastore_db?createDatabaseIfNotExist=true")
      //.config("javax.jdo.option.ConnectionUserName", "hiveuser")
      //.config("javax.jdo.option.ConnectionPassword", "password")
      //.config("spark.yarn.dist.files", "/opt/hadoop-2.8.5/hive/conf/hive-site.xml")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/hive/warehouse/").enableHiveSupport().getOrCreate();
    val d = ss.read.option("header",true).option("inferSchema",true).csv("/home/yuvatejav/Desktop/AC/data.csv").toDF().repartition(100)
    d.printSchema()
    var newDf: DataFrame =  d
    newDf.columns.foreach { col =>
      //println(col + " after column replace " + col.replaceAll(" ", ""))
      newDf = newDf.withColumnRenamed(col, col.replaceAll(" ", "")
      )
    }
    newDf.printSchema()
    //d.show()
    import ss.implicits._
    import ss.sql
    ss.sql("show databases").show()
    val l = d.select(d.col("*")).filter("Age <30").filter($"Preferred Foot" === "Left" ).filter($"Position"==="LM" || $"Position" ==="LCM" || $"Position" ==="LWM" ).filter($"Club" isNotNull ).groupBy("Club").agg(count("ID") alias("count")).sort(desc("count")).take(1)
    l.foreach(print)
    //d.createOrReplaceTempView("fifa")
    //val q1 = ss.sql("select club,count(*) from fifa where Age < 30 and `Preferred Foot`=\"Left\" and Club is not null and  Position=\"LM\"  group by club order by count(*) desc")
    //q1.show()

    //val da = d.withColumn("joined", to_date("joined", "MMM dd, yyyy"))
    val dc = newDf.withColumn("Wage", regexp_replace($"Wage","\\p{Sc}|K","" ).cast("Int") )
    dc.withColumn("Wage", col("Wage")*1000)
    val dq = dc.withColumn("Value", regexp_replace($"Value","\\p{Sc}|M","" ).cast("Int") )
    dc.withColumn("Value", col("Value")*1000000)

    //dc.na.fill("0",Seq("Wage")
    dc.show()
    dc.write.mode("overwrite").format("hive").saveAsTable("reporting.fifa19_report_table")
    //dc.printSchema()

    val qd = dc.groupBy("Position").agg(avg("Wage").alias("av")).sort(desc("av")).take(1)
    qd.foreach(print)
    //qd.createOrReplaceTempView("fifa1")
    //val s = ss.sql("select Position,max(av) from fifa1 group by Position, av ")
    //s.show()
    //.agg(min("Wage")) sum("Wage")
    //val qd1 = qd.groupBy("Position").max("av")
    //qd1.show()
    //qd1.printSchema()

    val dw = dq.groupBy("Club").agg(sum("Value").alias("val")).sort(desc("val")).take(1)
    dw.foreach(print)

    val de = dq.groupBy("Club").agg(sum("Wage").alias("val")).sort(desc("val")).take(1)

    dw.foreach(println)
    de.foreach(println)
    val y = dw.toList
    val z = de.toList
    println(y)
    println(z)
    println(y(0)(0))
    println(z(0)(0))
    val x = y(0)(0).equals(z(0)(0))
    println(x)

    val formation = List("GK","RB","RCB","LCB","LB","RM","LCM","RCM","LM","RF","ST")
    //val qt = dc.filter(col("Position") isin(formation)).groupBy("Nationality","Position").agg(avg("Overall").alias("rate")).sort(desc("rate")).take(1)
    val qt = dc.select(col("*")).filter(col("Position") isin(formation:_*)).groupBy("Position").agg(max("Overall") alias("moall") )
    //qt.foreach(print)
    //qt.show()
   // val qh = qt.join(dc)
    //qh.show()

    val pwf = dc.filter(col("Position") isin(formation:_*))
    val wf = Window.partitionBy("Position","Nationality").orderBy("Overall")
    val post = pwf.withColumn("rank",  dense_rank().over(wf))
    val prow = post.withColumn("rank",  row_number().over(wf))

    //prow.filter(col("rank") <2 ).sort("Nationality").show(40)
  }
}
