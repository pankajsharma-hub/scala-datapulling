package transformation

import java.util.Properties
import org.apache.spark.sql.functions.{udf,when,col,year,dayofmonth,month}
import org.apache.spark.sql
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkException
import org.apache.hadoop.conf.Configuration

object childgenderformat {
  
   val genderM = udf{ (s: String) =>
s match{
case "Male" | "male" | "m" | "mal" | "Mal" | "MALE" | "mALE" | "MaLe" | "MAL" =>
"M"
case _=> s
}
}

val genderF = udf{ (s: String) =>
s match{
case "Female" | "female" | "f" | "FEMALE" | "Femal" | "femal"| "FEMAl" | "FEMAL"=>
"F"
case _=> s
}
}
def main(args: Array[String]): Unit = {
 
  
      val conf = new SparkConf().setAppName("ChildGender Format")
     Class.forName("com.mysql.jdbc.Driver")
    val sc = new SparkContext(conf)
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val DF = sqlContext.read.format("delta").load("hdfs://masterHP:9000/delta/warehouse/HP_test/staging/child_details_stage2")
    DF.registerTempTable("child_details")
    val DF1= sqlContext.sql("Select * from child_details")
     DF1.show
     val genderDF = DF.withColumn("child_sex", genderM($"child_sex"))
     val genderDF2 = genderDF.withColumn("child_sex", genderF($"child_sex"))
  
     genderDF2.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP_test/staging/child_details_stage3")
  
}
}