package datapulling.mysqlpull

import java.util.Properties
import org.apache.spark.sql.{ SQLContext, _ }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.functions.{ concat, lit, col }
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode

object datapulling {
  def main(args: Array[String]): Unit = {
    
      val hconf = new Configuration()
    val fs = FileSystem.get(hconf)
    val path = new Path("hdfs://masterHP:9000/delta/warehouse/HP/raw")
    if (!fs.exists(path)) {
      fs.mkdirs(path)
    } 
//    else {
//      fs.delete(path)
//      fs.mkdirs(path)
//    }
//    val path1 = new Path("hdfs://masterHP:9000/delta/warehouse/HP/staging")
//    if (!fs.exists(path1)) {
//      fs.mkdirs(path1)
//    } else {
//      fs.delete(path1)
//      fs.mkdirs(path1)
//    }

//    val path1a = new Path("hdfs://masterHP:9000/delta/warehouse/HP/validation")
//    if (!fs.exists(path1a)) {
//      fs.mkdirs(path1a)
//    } else {
//      fs.delete(path1a)
//      fs.mkdirs(path1a)
//    }
     val mysql_pro = new java.util.Properties
    val conf = new SparkConf().setAppName("DataPullingFromHimachal")
    mysql_pro.setProperty("user", "hduser")
    mysql_pro.setProperty("password", "NRP123")
    Class.forName("com.mysql.jdbc.Driver")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
   import sqlContext.implicits._
   
    val conh = "jdbc:mysql://DELL:3306/IMMHP"
  
    // Pregnant Women Table
    val Pregnant = sqlContext.read.jdbc(conh, "pregnant_women", mysql_pro)
      Pregnant.registerTempTable("pregnant_details")
//  val women_query=sqlContext.sql("Select * from pregnant_details where pregnant_details.TS>date_sub(CAST(current_timestamp() as DATE), 1)")
  val women_query=sqlContext.sql("Select * from pregnant_details ")
     if(fs.exists(new Path("hdfs://masterHP:9000/delta/warehouse/HP/raw/pregnant_women"))){
            women_query.write.format("delta").mode("append").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/pregnant_women")
            }
             else{
            women_query.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/pregnant_women")
            }
   //Pregnant.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/pregnant_women")

     // ##Eligible Couple Table
     
    val Eligible = sqlContext.read.jdbc(conh, "eligible_couple", mysql_pro)
    Eligible.registerTempTable("eligible_details")
//    val couple_query=sqlContext.sql("Select * from eligible_details where eligible_details.TS>date_sub(CAST(current_timestamp() as DATE), 1)")
    val couple_query=sqlContext.sql("Select * from eligible_details")
    if(fs.exists(new Path("hdfs://masterHP:9000/delta/warehouse/HP/raw/pregnant_women"))){
            couple_query.write.format("delta").mode("append").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/eligible_couple")
            }
             else{
            couple_query.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/eligible_couple")
            }
   // Eligible.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/eligible_couple")
    
    // ##Mother Master Table 
    
    women_query.registerTempTable("pregnant_women")
    couple_query.registerTempTable("eligible_couple")
//    val mother= sqlContext.sql("select pregnant_women.rch_id_women as rch_mother_id,eligible_couple.women_name as mother_name,eligible_couple.age_at_regn,eligible_couple.husband_name,eligible_couple.women_education as mother_education,eligible_couple.women_occupation as mother_occupation,pregnant_women.dob as mother_dob,eligible_couple.husband_education,husband_occupation,family_annual_income,no_of_children, women_mobile as mother_mobile,husband_mobile,religion,caste,aadhar_number from eligible_couple,pregnant_women where pregnant_women.rch_id_women=eligible_couple.rch_id_women and eligible_couple.TS>date_sub(CAST(current_timestamp() as DATE), 1)");
      val mother= sqlContext.sql("select pregnant_women.rch_id_women as rch_mother_id,eligible_couple.women_name as mother_name,eligible_couple.age_at_regn,eligible_couple.husband_name,eligible_couple.women_education as mother_education,eligible_couple.women_occupation as mother_occupation,pregnant_women.dob as mother_dob,eligible_couple.husband_education,husband_occupation,family_annual_income,no_of_children, women_mobile as mother_mobile,husband_mobile,religion,caste,aadhar_number from eligible_couple,pregnant_women where pregnant_women.rch_id_women=eligible_couple.rch_id_women");

    val mother_mas=mother.withColumn("scheme_name",lit("No Scheme")).withColumn("ration_number",lit("NA"))
    mother_mas.show
            if(fs.exists(new Path("hdfs://masterHP:9000/delta/warehouse/HP/raw/mother_master"))){
            mother_mas.write.format("delta").mode("append").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/mother_master")
            }
             else{
            mother_mas.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/mother_master")
            }
           
    
    // mother_mas.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/mother_master")
    
    
    //val ANC = sqlContext.read.jdbc(conh, "anc_checkup", mysql_pro)
    //ANC.show

    //ANC.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/anc_checkup")
  
    // ## Child Detail table
    
    val Delivery = sqlContext.read.jdbc(conh, "delivery_outcome", mysql_pro)
    Delivery.registerTempTable("child_details")
   val child_query=sqlContext.sql("Select * from child_details ")
//    val child_query=sqlContext.sql("Select * from child_details where child_details.TS>date_sub(CAST(current_timestamp() as DATE), 1)")
    child_query.show
     if(fs.exists(new Path("hdfs://masterHP:9000/delta/warehouse/HP/raw/child_details"))){
            child_query.write.format("delta").mode("append").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/child_details")
            }
             else{
            child_query.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/child_details")
            }
   //Delivery.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/child_details")

    
    // ## Vaccine Detail Table
    
   val Vaccine = sqlContext.read.jdbc(conh, "vaccine_details", mysql_pro)
    
     Vaccine.registerTempTable("vaccine_details")
//     val vaccine_query=sqlContext.sql("Select * from vaccine_details where vaccine_details.TS>date_sub(CAST(current_timestamp() as DATE), 1)")
      val vaccine_query=sqlContext.sql("Select * from vaccine_details")
      vaccine_query.show
     if(fs.exists(new Path("hdfs://masterHP:9000/delta/warehouse/HP/raw/vaccine_details"))){
            vaccine_query.write.format("delta").mode("append").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/vaccine_details")
            }
             else{
            vaccine_query.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/vaccine_details")
            }
    //Vaccine.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/vaccine_details")
    
   // ## Child vaccination Table
   
    val Child = sqlContext.read.jdbc(conh, "child_vaccination", mysql_pro)
    
      Child.registerTempTable("child_vaccination_details")
//     val vaccine_child=sqlContext.sql("Select * from child_vaccination_details where child_vaccination_details.TS>date_sub(CAST(current_timestamp() as DATE), 1)")
     val vaccine_child=sqlContext.sql("Select * from child_vaccination_details")
      vaccine_child.show
     if(fs.exists(new Path("hdfs://masterHP:9000/delta/warehouse/HP/raw/child_vaccination_details"))){
            vaccine_child.write.format("delta").mode("append").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/child_vaccination_details")
            }
             else{
            vaccine_child.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/child_vaccination_details")
            }
    //Child.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/child_vaccination_details")

       // ## ANM Details
    
      val ANM = sqlContext.read.jdbc(conh, "anm_details", mysql_pro)
      ANM.registerTempTable("ANM_details")
     val anm_det=sqlContext.sql("Select * from ANM_details ")
//      val anm_det=sqlContext.sql("Select * from ANM_details where ANM_details.TS>date_sub(CAST(current_timestamp() as DATE), 1)")
      anm_det.show
     if(fs.exists(new Path("hdfs://masterHP:9000/delta/warehouse/HP/raw/anm_details"))){
            anm_det.write.format("delta").mode("append").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/anm_details")
            }
             else{
            anm_det.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/anm_details")
            }
  
      
     // ANM.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/anm_details")
      
      // ##ANM mapping
      
    anm_det.registerTempTable("anm_detail")
    val Facilitydf = anm_det.withColumn("facility_id", concat(lit("02"),col("facility_id")))
    Facilitydf.registerTempTable("anm_details")
    val anmfacility = sqlContext.sql("Select distinct facility_id,anm_id,join_date from anm_details")
 if(!anmfacility.isEmpty){
    if(fs.exists(new Path("hdfs://masterHP:9000/delta/warehouse/HP/validation/nrp_anm_facility_mapping"))){
      anmfacility.write.format("delta").mode("append").save("hdfs://masterHP:9000/delta/warehouse/HP/validation/nrp_anm_facility_mapping")
        }
        else{
        anmfacility.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/validation/nrp_anm_facility_mapping")
        }
 }
    // ##Facility Details
    
    val Facility = sqlContext.read.jdbc(conh, "facility_details", mysql_pro)
     Facility.registerTempTable("facility_details")
     val fac_det=sqlContext.sql("Select * from facility_details ")
//       val fac_det=sqlContext.sql("Select * from facility_details where facility_details.TS>date_sub(CAST(current_timestamp() as DATE), 1)")
      fac_det.show
      if(fs.exists(new Path("hdfs://masterHP:9000/delta/warehouse/HP/raw/facility_details"))){
            fac_det.write.format("delta").mode("append").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/facility_details")
            }
             else{
            fac_det.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/facility_details")
            }

    //Facility.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/facility_details")

     // ##ASHA details
    val ASHA = sqlContext.read.jdbc(conh, "asha_details", mysql_pro)
     ASHA.registerTempTable("ASHA_details")
     val asha_det=sqlContext.sql("Select * from ASHA_details ")
//      val asha_det=sqlContext.sql("Select * from ASHA_details where ASHA_details.TS>date_sub(CAST(current_timestamp() as DATE), 1)")
      asha_det.show
      if(fs.exists(new Path("hdfs://masterHP:9000/delta/warehouse/HP/raw/asha_details"))){
            asha_det.write.format("delta").mode("append").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/asha_details")
            }
             else{
            asha_det.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/raw/asha_details")
           }
    // ##ASHA mapping
    asha_det.registerTempTable("asha_detail")
    val Facilitydf1 = asha_det.withColumn("facility_id", concat(lit("02"),col("facility_id")))
    Facilitydf1.registerTempTable("ASHA_details")
    val ashafacility = sqlContext.sql("Select distinct facility_id,asha_id,join_date from ASHA_details")
if(!ashafacility.isEmpty){
    if(fs.exists(new Path("hdfs://masterHP:9000/delta/warehouse/HP/validation/nrp_asha_facility_mapping"))){
      ashafacility.write.format("delta").mode("append").save("hdfs://masterHP:9000/delta/warehouse/HP/validation/nrp_asha_facility_mapping")
        }
        else{
        ashafacility.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/validation/nrp_asha_facility_mapping")
         }
}
    
  // ashafacility.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/HP/validation/nrp_asha_facility_mapping")

//------------------------------------Data_Pull_ID---------------------------------//
//val mysql_props1 = new java.util.Properties
//    mysql_props1.setProperty("user", "hduser")
//    mysql_props1.setProperty("password", "NRP123")
//val date_time = sqlContext.sql("SELECT CURRENT_TIMESTAMP()").collect()(0)(0).toString.split(" ")
//    val time = date_time(1).split(":")
//    val ts = date_time(0) + "_" + time(0) + "-" + time(1)
// val dag_run_time = Seq(("SQL_DateLake_HP", ts))
//    val start_run_rdd = sqlContext.sparkContext.parallelize(dag_run_time).toDF("dag_id", "run_time")
//    start_run_rdd.show
//    start_run_rdd.write.mode(SaveMode.Append).jdbc("jdbc:mysql://masterTN/nrp", "dag_log", mysql_props1)
    
  }
}
