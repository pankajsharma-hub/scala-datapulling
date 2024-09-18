package transformation
import java.util.Properties
import org.apache.spark.sql.functions.{concat,lit,col}
import org.apache.spark.sql
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object Facility_Idgen {
  
  def main(args: Array[String]): Unit = {
    
     val Conf = new SparkConf().setAppName("Facility_Id_Generation")
  val sc = new SparkContext(Conf)
  val spark=new org.apache.spark.sql.SQLContext(sc)
  import spark.implicits._
  
   val DF = spark.read.format("delta").load("hdfs://masterHP:9000/delta/warehouse/HP_test/raw/facility_details")
  DF.registerTempTable("facility_master")
  val Facilitydf = DF.withColumn("facility_id", concat(lit("02"),col("facility_id"))).withColumn("state",lit("HimachalPradesh"))
  Facilitydf.show
  
  Facilitydf.registerTempTable("Facility_master")
  val facility=spark.sql("Select facility_id,facility_name,facility_type, state, district,block,phc, sc, village from Facility_master")
  if(!facility.isEmpty){
   facility.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP_test/validation/nrp_facility_master")
  }
   facility.show
 
    
//    val hconf = new Configuration()
//    val fs = FileSystem.get(hconf)
//    val path = new Path("hdfs://masterTN:9000/delta/warehouse/HP_test/validation/HP_test/nrp_facility_master")
//    if (fs.exists(path)) {
//      facility.write.mode("append").format("delta").save("hdfs://masterTN:9000/delta/warehouse/NRP/validation/HP_test/nrp_facility_master")
//    }
//    else{
//      facility.write.format("delta").save("hdfs://masterTN:9000/delta/warehouse/NRP/validation/HP_test/nrp_facility_master")
//    }
//  

  
  
  
  }
}