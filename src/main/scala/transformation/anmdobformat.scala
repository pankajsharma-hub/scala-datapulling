package transformation
import java.util.Properties
import org.apache.spark.sql.functions.{udf,when,col,year,dayofmonth,month}
import org.apache.spark.sql
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkException
import org.apache.hadoop.conf.Configuration

object anmdobformat {
  
    val currentAge = udf{ (dob: java.sql.Date) =>
import java.util.Calendar
import java.text.SimpleDateFormat
val dateTime = dob
val dateFormat = new SimpleDateFormat("dd-MMM-yyyy")
dateFormat.format(dateTime)
}
    
    def main(args: Array[String]): Unit = {
      
        val conf = new SparkConf().setAppName("ANM Date ID Generation")
     Class.forName("com.mysql.jdbc.Driver")
    val sc = new SparkContext(conf)
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val DF = sqlContext.read.format("delta").load("hdfs://masterHP:9000/delta/warehouse/HP/raw/anm_details")
     DF.registerTempTable("anm_details")
    val DF1= sqlContext.sql("Select * from anm_details")
     DF1.show
    val DOBdf = DF.withColumn("dob", when($"dob".isNotNull, currentAge($"dob"))).withColumn("join_date",when($"join_date".isNotNull, currentAge($"join_date")))
  
     

    DOBdf.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/staging/anm_details_stage1")
 
}
  
}