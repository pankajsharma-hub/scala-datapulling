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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path


object motherdobformat {
  
   val hconf = new Configuration()
    val fs = FileSystem.get(hconf)
  
   val currentAge = udf{ (dob: java.sql.Date) =>
import java.util.Calendar
import java.text.SimpleDateFormat
val dateTime = dob
val dateFormat = new SimpleDateFormat("dd-MMM-yyyy")
dateFormat.format(dateTime)
}
    
    def main(args: Array[String]): Unit = {
      
        val conf = new SparkConf().setAppName("MotherDOB Generation")
     Class.forName("com.mysql.jdbc.Driver")
    val sc = new SparkContext(conf)
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    
    
    val Eligible = sqlContext.read.format("delta").load("hdfs://masterHP:9000/delta/warehouse/HP_test/raw/eligible_couple")
     Eligible.registerTempTable("eligible_details")
     
       val Pregnant = sqlContext.read.format("delta").load("hdfs://masterHP:9000/delta/warehouse/HP_test/raw/pregnant_women")
Pregnant.registerTempTable("pregnant_details");
        
          val women_query=sqlContext.sql("Select * from pregnant_details ")

        
//    val couple_query=sqlContext.sql("Select * from eligible_details where eligible_details.TS>date_sub(CAST(current_timestamp() as DATE), 1)")
    val couple_query=sqlContext.sql("Select * from eligible_details")
   
    // ##Mother Master Table 
    
    women_query.registerTempTable("pregnant_women")
    couple_query.registerTempTable("eligible_couple")
//    val mother= sqlContext.sql("select pregnant_women.rch_id_women as rch_mother_id,eligible_couple.women_name as mother_name,eligible_couple.age_at_regn,eligible_couple.husband_name,eligible_couple.women_education as mother_education,eligible_couple.women_occupation as mother_occupation,pregnant_women.dob as mother_dob,eligible_couple.husband_education,husband_occupation,family_annual_income,no_of_children, women_mobile as mother_mobile,husband_mobile,religion,caste,aadhar_number from eligible_couple,pregnant_women where pregnant_women.rch_id_women=eligible_couple.rch_id_women and eligible_couple.TS>date_sub(CAST(current_timestamp() as DATE), 1)");
      val mother= sqlContext.sql("select pregnant_women.rch_id_women as rch_mother_id,eligible_couple.women_name as mother_name,eligible_couple.age_at_regn,eligible_couple.husband_name,eligible_couple.women_education as mother_education,eligible_couple.women_occupation as mother_occupation,pregnant_women.dob as mother_dob,eligible_couple.husband_education,husband_occupation,family_annual_income,no_of_children, women_mobile as mother_mobile,husband_mobile,religion,caste,aadhar_number from eligible_couple,pregnant_women where pregnant_women.rch_id_women=eligible_couple.rch_id_women");

    val mother_mas=mother.withColumn("scheme_name",lit("No Scheme")).withColumn("ration_number",lit("NA"))
    mother_mas.show
            if(fs.exists(new Path("hdfs://masterHP:9000/delta/warehouse/HP_test/raw/mother_master"))){
            mother_mas.write.format("delta").mode("append").save("hdfs://masterHP:9000/delta/warehouse/HP_test/raw/mother_master")
            }
             else{
            mother_mas.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP_test/raw/mother_master")
            }
           
    if(fs.exists(new Path("hdfs://masterHP:9000/delta/warehouse/HP_test/raw/mother_master"))){
val DF = sqlContext.read.format("delta").load("hdfs://masterHP:9000/delta/warehouse/HP_test/raw/mother_master")
    
         DF.registerTempTable("mother_master")
    }else{
      print("mother master does not exists.!");
    }

val child_DF = sqlContext.read.format("delta").load("hdfs://masterHP:9000/delta/warehouse/HP_test/raw/child_details")
     child_DF.registerTempTable("child_master")

val DF1=sqlContext.sql("Select mother_master.rch_mother_id, mother_master.mother_name,mother_master.mother_dob,mother_master.age_at_regn,mother_master.husband_name,mother_master.mother_education,mother_master.mother_occupation,mother_master.husband_education,mother_master.husband_occupation,mother_master.family_annual_income,mother_master.no_of_children,mother_master.mother_mobile,mother_master.husband_mobile,mother_master.religion,caste,ration_number,scheme_name,aadhar_number from mother_master, child_master where mother_master.rch_mother_id=child_master.rch_id_women")


    val DOBdf = DF1.withColumn("DF1", when($"mother_dob".isNotNull, currentAge($"mother_dob")))
   
     DOBdf.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP_/staging/mother_master_stage1")
   
}
}
