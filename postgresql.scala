import java.sql.DriverManager
import java.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession, UDFRegistration}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.unix_timestamp

object postgresql {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("postgresql").master("local[*]")
     // .config("Driver","org.postgresql.Driver")
      .getOrCreate();

    val url = "jdbc:postgresql://localhost:5432/test_db"
    val driver = "org.postgresql.Driver"
    val query1 = "(SELECT * FROM t1) as q1"
    val cp = new Properties()
    cp.setProperty("Driver", "org.postgresql.Driver")
    cp.setProperty("user","postgres")
    cp.setProperty("password","postgres")

    //Class.forName("org.postgresql.Driver").newInstance
    //val db = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test_db","postgres","postgres")
    val rs = ss.read.jdbc(url,query1,cp)

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
    val d = ss.read.option("header",true).option("inferSchema",true).csv("/home/yuvatejav/Desktop/AC/data.csv").toDF().repartition(100)
    d.printSchema()
    var newDf: DataFrame =  d
    newDf.columns.foreach { col =>
      //println(col + " after column replace " + col.replaceAll(" ", ""))
      newDf = newDf.withColumnRenamed(col, col.replaceAll(" ", ""))
    }

    val reg = ss.udf.register("fconvertWageToInt",convertWageToInt(_:String))
    val d1 =  newDf.withColumn("Wage",reg(col("Wage")) )
    // val reg1 = ss.udf.register("fconvertWageToInt",convertWageToInt(_:String))
    val d2 = d1.withColumn("Value",reg(col("Value")) )

    val d3 = d2.withColumn("Joined", to_date(from_unixtime(unix_timestamp(col("Joined"),"MMM dd, yyyy"))) )

    d3.write.mode("overwrite").jdbc(url=url,table="Fifa_dataset",connectionProperties = cp)

  }

}
