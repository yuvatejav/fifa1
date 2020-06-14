import org.apache.hadoop.hive.metastore.api.Date
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.{DataFrame, SparkSession, UDFRegistration}
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.hadoop.hive.metastore.api.Date
import org.apache.spark.sql.expressions.Window


object Test {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("LFM_LT30").master("local[*]")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      //.config("hive.enforce.bucketing","true")
      //.config("hive.vectorized.execution.enabled","true")
      //.config("hive.exec.dynamic.partition", "true")
      //.config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/hive/warehouse").enableHiveSupport().getOrCreate();

    import ss.implicits._

    val d = ss.read.option("header",true).option("inferSchema",true).csv("/home/yuvatejav/Desktop/AC/data.csv").toDF().repartition(100)
    d.printSchema()
    var newDf: DataFrame =  d
    newDf.columns.foreach { col =>
      //println(col + " after column replace " + col.replaceAll(" ", ""))
      newDf = newDf.withColumnRenamed(col, col.replaceAll(" ", "")
      )
    }
    newDf.printSchema()
    //d.createOrReplaceTempView("fif")
    //d.show()
    import ss.implicits._
    //insert into reporting.fifa19_report_table select

    def convertWageToInt(wage: String): Double= {

      val length  = wage.length()
      //println(length)
      if (wage.slice(1,length) =="0"){
        //println(wage.slice(1,length))
        //println("ZERO currency")
        return 0
      }
      var exp = wage(length-1)
      //println(exp)
      //Removing currency symbol
      val fin = wage.slice(1,length-1)
      //println(fin)
      var fi = fin.toDouble
      if(exp == 'K' || exp =='k' )    {
        //println("Inside if condition")
        fi = fi*1000
      }
      else if (exp == 'M' || exp =='m') {
        //println("Inside if else")
        fi = fi*1000000
      }
      return fi.toDouble
    }

    val reg = ss.udf.register("fconvertWageToInt",convertWageToInt(_:String))
    val d1 =  newDf.withColumn("Wage",reg(col("Wage")) )
   // val reg1 = ss.udf.register("fconvertWageToInt",convertWageToInt(_:String))
    val d2 = d1.withColumn("Value",reg(col("Value")) )
    val d3 = d2.withColumn("Joined", to_date(from_unixtime(unix_timestamp(col("Joined"),"MMM dd, yyyy"))) )

    //Question2
    val formation = List("GK","RB","RCB","LCB","LB","RM","LCM","RCM","LM","RF","ST")
    val pwf = d2.filter(col("Position") isin(formation:_*))
    val wf = Window.partitionBy("position").orderBy(desc("overall"))
    val post = pwf.withColumn("rank",  dense_rank().over(wf))
    //val prow = post.withColumn("rank",  row_number().over(wf))
    post.filter(col("rank")<2).select("name","nationality","club","value","wage","overall","position","rank").show()

    //Question 3
    val dw = d2.groupBy("Club").agg(sum("Value").alias("val")).sort(desc("val")).take(1)
    dw.foreach(print)

    val de = d2.groupBy("Club").agg(sum("Wage").alias("val")).sort(desc("val")).take(1)

    println("question3")
    dw.foreach(println)
    de.foreach(println)
    val y = dw.toList
    val z = de.toList
    println(y)
    println(z)
    println(y(0))
    println(z(0))
    println(y(0)(0))
    println(z(0)(0))
    val x = y(0)(0).equals(z(0)(0))
    println(x)

    //QUestion 4

    val qd = d2.groupBy("Position").agg(avg("Wage").alias("av")).sort(desc("av")).take(1)
    qd.foreach(print)
    println("question4")
    //ss.sql("insert into ")
   //val d2= d.withColumn("convertedWage",convertWageToInt(col("wage")))
    //val s =d.select(convertWageToInt(col("Wage") as "wa"))
    //val dc = d.withColumn("Wage", regexp_replace($"Wage","\\p{Sc}|K","" ).cast("Int") )
   // dc.withColumn("Wage", col("Wage")*1000)
    //val dq = dc.withColumn("Value", regexp_replace($"Value","\\p{Sc}|M","" ).cast("Int") )
    //dq.withColumn("Value", col("Value")*1000000)

   // d3.write.mode("overwrite")
     // .partitionBy("Club").saveAsTable("reporting.fifa19_report_table3")

    //Rank


  }

}
