package transformation

import java.util.Properties
import org.apache.spark.sql.functions.{udf,when,col,year,dayofmonth,month}
import org.apache.spark.sql
import org.apache.spark.sql.SaveMode
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkException
import org.apache.hadoop.conf.Configuration

object childbirthweight {
  
    val weight_range = udf{ (wgt: Float) =>
var w = wgt
if (wgt < 0)
{
  import scala.math.abs 
w=Math.abs(w)

} 
w.toFloat 
}

def main(args: Array[String]): Unit = {
 
  
      val conf = new SparkConf().setAppName("ChildBirth Weight")
     Class.forName("com.mysql.jdbc.Driver")
    val sc = new SparkContext(conf)
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val DF = sqlContext.read.format("delta").load("hdfs://masterHP:9000/delta/warehouse/HP_test/staging/child_details_stage1")
    DF.registerTempTable("child_details")
    val DF1= sqlContext.sql("Select * from child_details")
     DF1.show

    // val WGTdf = DF1.withColumn("birth_weight", when($"birth_weight".isNotNull, weight_range($"birth_weight")))
    //  WGTdf.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP_test/staging/child_details_stage2")
     
     //Transformed_Records

    val count=DF.filter($"birth_weight"<0).count

    val connrp = "jdbc:mysql://masterTN/nrp"
    val mysql_props1 = new java.util.Properties
    mysql_props1.setProperty("user", "hduser")
    mysql_props1.setProperty("password", "NRP123")

    val dag_log = sqlContext.read.jdbc(connrp, "dag_log", mysql_props1)
    dag_log.registerTempTable("dag_log")
    val dag_log_df = sqlContext.sql("select * from dag_log order by run_time")
    val n = dag_log_df.count().toInt
    val ts = dag_log_df.collect()(n - 1)(2).toString

    val Transformed_Records = count
    val PreProcessed_Table = sqlContext.read.jdbc(connrp, "PreProcessed_Table", mysql_props1)
    PreProcessed_Table.registerTempTable("PreProcessed_Table")
    val Data_Pull_ID = ts
    val Table_Name = "IMMHP.child_master"
    val Category = "Data Plausibility Check"
    val data = Seq((Table_Name, Category, Transformed_Records, Data_Pull_ID))
    val Transformed_data = sqlContext.sparkContext.parallelize(data).toDF("Table_Name", "Category", "Transformed_Records", "Data_Pull_ID")
    Transformed_data.show
    Transformed_data.write.mode(SaveMode.Append).jdbc("jdbc:mysql://masterTN/nrp", "PreProcessed_Table", mysql_props1)

     val WGTdf = DF1.withColumn("birth_weight", when($"birth_weight".isNotNull, weight_range($"birth_weight")))
 
      WGTdf.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP_test/staging/child_details_stage2")
   
}
}
