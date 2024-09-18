package transformation

import java.util.Properties
import org.apache.spark.sql.functions.{udf,when,col,year,dayofmonth,month}
import org.apache.spark.sql
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkException
import org.apache.hadoop.conf.Configuration

object vaccinedateid {
  
    val current = udf{ (dob: java.sql.Date) =>
import java.util.Calendar
import java.text.SimpleDateFormat
val dateTime = dob
val dateFormat = new SimpleDateFormat("dd-MMM-yyyy")
dateFormat.format(dateTime)
}
    
    def main(args: Array[String]): Unit = {
      
        val conf = new SparkConf().setAppName("Vccine Date ID Generation")
     Class.forName("com.mysql.jdbc.Driver")
    val sc = new SparkContext(conf)
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val DF = sqlContext.read.format("delta").load("hdfs://masterHP:9000/delta/warehouse/HP_test/raw/vaccine_details")
     DF.registerTempTable("Vaccine_details")
    val DF1= sqlContext.sql("Select * from Vaccine_details")
     DF1.show
    val DOBdf = DF1.withColumn("date_of_issue", when($"date_of_issue".isNotNull, current($"date_of_issue"))).withColumn("expiry_date",when($"expiry_date".isNotNull, current($"expiry_date")))
     DOBdf.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP_test/staging/vaccine_details_stage1")
     
      DOBdf.registerTempTable("Vaccine_stock")
      val stock=sqlContext.sql("Select vaccine_id,batch_no, date_of_issue,expiry_date from vaccine_stock")
      if(!stock.isEmpty){
      stock.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP_test/validation/nrp_vaccine_stock")  
      }
}
  
}