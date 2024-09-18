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

object mothermobilecheck {
  
  
val mobile_chk = udf { (mob: String) =>
    var s = mob
    var n = s.length - 10
    if (n <= 0) {
      s
    } else {
      val t = s.slice(0, n)
      if (t == "+91" || t == "91" || t == "0" || t == "(91)" || t == "091" || t == "91-" || t == "91 ") {
        s.slice(n, 50)
      } else {
        s
      }
    }
  }
 //Transformed_Records Count

val mobile_chk_count = udf { (mob: String) =>
    var s = mob
    var n = s.length - 10
    if (n <= 0) {
      0
    } else {
      val t = s.slice(0, n)
      if (t == "+91" || t == "91" || t == "0" || t == "(91)" || t == "091" || t == "91-" || t == "91 ") {
        1
      } else {
        0
      }
    }
  }
  
  def main(args: Array[String]): Unit = {
    
      
     val conf = new SparkConf().setAppName("EC MobileNo")
     Class.forName("com.mysql.jdbc.Driver")
    val sc = new SparkContext(conf)
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val mother = sqlContext.read.format("delta").load("hdfs://masterHP:9000/delta/warehouse/HP_test/staging/mother_master_stage1")
    mother.registerTempTable("mother_master1")
    val ECM= sqlContext.sql("Select * from mother_master1")
    val countdf1 = ECM.withColumn("count", when($"mother_mobile".isNotNull, mobile_chk_count($"mother_mobile")))
    val count_df1 = countdf1.filter(countdf1("count") === 1 ).count
     val countdf2 = ECM.withColumn("count", when($"mother_mobile".isNotNull, mobile_chk_count($"mother_mobile")))
    val count_df2 = countdf2.filter(countdf2("count") === 1 ).count
    val mobilecheck = ECM.withColumn("mother_mobile", when($"mother_mobile".isNotNull, mobile_chk($"mother_mobile"))).withColumn("husband_mobile", when($"husband_mobile".isNotNull, mobile_chk($"husband_mobile")))
    
    //Transformed_Records

    val connrp = "jdbc:mysql://masterTN/nrp"
    val mysql_props1 = new java.util.Properties
    mysql_props1.setProperty("user", "hduser")
    mysql_props1.setProperty("password", "NRP123")

    val dag_log = sqlContext.read.jdbc(connrp, "dag_log", mysql_props1)
    dag_log.registerTempTable("dag_log")
    val dag_log_df = sqlContext.sql("select * from dag_log order by run_time")
    val n = dag_log_df.count().toInt
    val ts = dag_log_df.collect()(n - 1)(2).toString

    val Transformed_Records = count_df1+count_df2
    val PreProcessed_Table = sqlContext.read.jdbc(connrp, "PreProcessed_Table", mysql_props1)
    PreProcessed_Table.registerTempTable("PreProcessed_Table")
    val Data_Pull_ID = ts
    val Table_Name = "IMMHP.pregnant_women"
    val Category = "Data Accuracy Mobile"
    val data = Seq((Table_Name, Category, Transformed_Records, Data_Pull_ID))
    val Transformed_data = sqlContext.sparkContext.parallelize(data).toDF("Table_Name", "Category", "Transformed_Records", "Data_Pull_ID")
    Transformed_data.show
    Transformed_data.write.mode(SaveMode.Append).jdbc("jdbc:mysql://masterTN/nrp", "PreProcessed_Table", mysql_props1)
 
    mobilecheck.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP_test/staging/mother_master_stage2")
  

  }
}
