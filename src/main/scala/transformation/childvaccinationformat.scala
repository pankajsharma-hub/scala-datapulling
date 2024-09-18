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
object childvaccinationformat {
  
      val currentdate = udf{ (dob: java.sql.Date) =>
import java.util.Calendar
import java.text.SimpleDateFormat
val dateTime = dob
val dateFormat = new SimpleDateFormat("dd-MMM-yyyy")
dateFormat.format(dateTime)
    }
def main(args: Array[String]): Unit = {
  val conf = new SparkConf().setAppName("VaccineDate Format")
     Class.forName("com.mysql.jdbc.Driver")
    val sc = new SparkContext(conf)
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
   val child = sqlContext.read.format("delta").load("hdfs://masterHP:9000/delta/warehouse/HP_test/raw/child_vaccination_details")
    
    child.registerTempTable("Child_Vaccination")
    val Facilitydf = child.withColumn("facility_id", concat(lit("02"),col("facility_id")))
  
  Facilitydf.registerTempTable("child_vaccination")
    val DOV =sqlContext.sql("Select * from child_vaccination")
     DOV.show
    val CDOV=DOV.withColumn("date_of_vaccination", when($"date_of_vaccination".isNotNull, currentdate($"date_of_vaccination")))
    if(!CDOV.isEmpty){
     CDOV.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP_test/validation/nrp_child_immunization")
    }
     CDOV.show
}
}