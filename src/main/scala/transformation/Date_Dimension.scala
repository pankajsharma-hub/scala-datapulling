package transformation

import java.util.Properties
import org.apache.spark.sql.functions.{udf,when,col,year,dayofmonth,month}
import org.apache.spark.sql
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object Date_Dimension {
  
   val date_day = udf{ (Date_id: java.sql.Date) =>
import java.util.Calendar
import java.text.SimpleDateFormat
val dateTime = Date_id
val dateFormat = new SimpleDateFormat("EEE")
dateFormat.format(Date_id)
}

  val date_id = udf{ (Date_id: java.sql.Date) =>
  import java.util.Calendar
  import java.text.SimpleDateFormat
  val dateTime = Date_id
  val dateFormat = new SimpleDateFormat("ddMMyyyy")
  dateFormat.format(Date_id).toString
}

  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Date ID Generation")
     Class.forName("com.mysql.jdbc.Driver")
    val sc = new SparkContext(conf)
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val DF = sqlContext.read.format("delta").load("hdfs://masterHP:9000/delta/warehouse/HP_test/raw/child_vaccination_details")
    
    
    DF.registerTempTable("child_vaccination")
    val DF1= sqlContext.sql("Select distinct date_of_vaccination from child_vaccination")
      
     
    val DateDF = DF1.withColumn("date_id",date_id($"date_of_vaccination")).withColumn("year_value",year(col("date_of_vaccination"))).withColumn("month_value",month(col("date_of_vaccination"))).withColumn("day_value",date_day($"date_of_vaccination")).withColumn("date_value", dayofmonth(col("date_of_vaccination"))).withColumn("holiday",when(col("day_value")==="Sun","yes").otherwise("no"))
    DateDF.show
  
    DateDF.registerTempTable("Date_Table")
    
    val dateid=sqlContext.sql("Select date_of_vaccination,date_id, year_value, month_value,date_value,day_value as day_of_date,holiday from Date_Table")
    if(!dateid.isEmpty){
     dateid.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP_test/validation/nrp_date")
    }
       //val hconf=new Configuration()
  /*val fs = FileSystem.get(hconf)
  val path=new Path("hdfs://master:9000/delta/warehouse/HP_test/datadimension")
  if(!fs.exists(path)){
    dateid.write.format("delta").save("hdfs://master:9000/delta/warehouse/HP_test/datadimension")
  }
  else {
    dateid.write.format("delta").mode("append").save("hdfs://master:9000/delta/warehouse/HP_test/datadimension")
  }
     */
     
     
  }
}