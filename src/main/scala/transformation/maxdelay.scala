package transformation
import java.util.Properties
import org.apache.spark.sql.functions.{when,concat,lit,col}
import org.apache.spark.sql
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem


object maxdelay {
  
  def main(args: Array[String]): Unit = {
      
        val conf = new SparkConf().setAppName("Vaccine Max Delay")
     Class.forName("com.mysql.jdbc.Driver")
    val sc = new SparkContext(conf)
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val DF = sqlContext.read.format("delta").load("hdfs://masterHP:9000/delta/warehouse/HP_test/staging/vaccine_details_stage1")
     DF.registerTempTable("Vaccine_details")
    val vaccine_details= sqlContext.sql("Select vaccine_id,vaccine_name,when_to_use,max_delay,vaccine_type,vaccine_unit as vaccine_dosage from Vaccine_details")
     vaccine_details.show
    val df=vaccine_details.withColumn("max_delay",when($"vaccine_name"=== "BCG",lit("4 months")).otherwise($"max_delay"))
    df.show
    if(!df.isEmpty){
    df.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP_test/validation/nrp_vaccine_master")
    }
     
}
}