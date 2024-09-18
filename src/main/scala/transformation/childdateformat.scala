package transformation

import java.util.Properties
import org.apache.spark.sql.functions.{udf,when,col,year,dayofmonth,month}
import org.apache.spark.sql
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkException
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.functions.{ concat, lit, col }
object childdateformat {
  
      val currentAge = udf{ (dob: java.sql.Date) =>
import java.util.Calendar
import java.text.SimpleDateFormat
val dateTime = dob
val dateFormat = new SimpleDateFormat("dd-MMM-yyyy")
dateFormat.format(dateTime)
}
    
    def main(args: Array[String]): Unit = {
      
        val conf = new SparkConf().setAppName("Child Date Generation")
     Class.forName("com.mysql.jdbc.Driver")
    val sc = new SparkContext(conf)
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val DF = sqlContext.read.format("delta").load("hdfs://masterHP:9000/delta/warehouse/HP_test/raw/child_details")
     DF.registerTempTable("Child_details")
     val childdet = DF.withColumn("facility_id", concat(lit("02"),col("facility_id")))
    childdet.registerTempTable("child_details")
    
    val DF1= sqlContext.sql("Select rch_id_child as rch_child_id,child_name,rch_id_women as rch_mother_id,date_of_reg as enrollment_date,sex_of_infant as child_sex, dob as child_dob, birth_weight,facility_id as delivery_facility_id,facility_id as resident_facility_id from child_details")
     DF1.show
    val DOBdf = DF1.withColumn("child_dob", when($"child_dob".isNotNull, currentAge($"child_dob"))).withColumn("enrollment_date",when($"enrollment_date".isNotNull, currentAge($"enrollment_date")))
  
    DOBdf.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP_test/staging/child_details_stage1")
  
    DOBdf.show
    }
}