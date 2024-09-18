package datapulling.mysqlpull

import java.util.Properties
import org.apache.spark.sql.{ SQLContext, _ }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.functions.{ concat, lit, col }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.File
import java.sql.Timestamp

object HPpull {
 // jdbc:mysql://yourserver:3306/yourdatabase?zeroDateTimeBehavior=convertToNull

  val con = "jdbc:mysql://masterHP/HP_TEST?zeroDateTimeBehavior=convertToNull&autoReconnect=true&useSSL=false"
  val hconf = new Configuration()
  val fs = FileSystem.get(hconf)
  val mysql_props = new java.util.Properties
  mysql_props.setProperty("user", "hduser")
  mysql_props.setProperty("password", "NRP123") // TN Server
  Class.forName("com.mysql.jdbc.Driver")
  val Conf = new SparkConf().setAppName("Data Pulling from Himachal Pradesh").set("spark.cassandra.connection.host", "mastertn")
  val sc = new SparkContext(Conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val dag_inf = sqlContext.read.format("org.apache.spark.sql.cassandra").option("spark.cassandra.connection.host", "mastertn").options(Map("table" -> "dag_inf", "keyspace" -> "dqa_test_repository")).load()

  def pulling(table_name: String, ts_col_no: Int, datapullid: String): Unit = {

    val log = dag_inf.filter($"tablename" === table_name)

    if (log.isEmpty) {
      val raw_table = sqlContext.read.jdbc(con, table_name, mysql_props)
      raw_table.registerTempTable("raw_table")
      val raw_stage = sqlContext.sql("Select * from raw_table order by update_ts")
      val n = raw_stage.count().toInt
      val ts = raw_stage.collect()(n - 1)(ts_col_no).toString
      val df = raw_stage.filter($"create_ts" <= ts)
      df.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/Tracking/HP_test/raw/" + table_name)
      //      raw_table.write.format("delta").save("hdfs://masterTN:9000/delta/warehouse/Tracking/TN/raw/" + table_name)
      val today_date = sqlContext.sql("SELECT CURRENT_TIMESTAMP()").collect()(0)(0).toString.split(" ")(0)
      val data = Seq(("SQL_DataLake_TN", today_date, table_name, ts))
      val rdd = sqlContext.sparkContext.parallelize(data).toDF("dagid", "dagrundate", "tablename", "ts")
      //val rdd1=rdd.withColumn("dagrundate", 'dagrundate.cast("Date"))
      rdd.write.format("org.apache.spark.sql.cassandra").mode("append").option("spark.cassandra.connection.host", "mastertn").options(Map("table" -> "dag_inf", "keyspace" -> "dqa_test_repository")).save()

      //No of Records Pulled
      val State = "TN"
      //val District="Kancheepuram"
      val DateTime = sqlContext.sql("SELECT CURRENT_TIMESTAMP()").collect()(0)(0).toString.split(" ")
      val Date = ts.toString.split(" ")(0)
      val PulledRecords = raw_table.count.toInt
      val pull_data = Seq((datapullid, State, Date, table_name, PulledRecords))
      val rdd_pull = sqlContext.sparkContext.parallelize(pull_data).toDF("pullid", "state", "date", "tablename", "pulledrecords")
      rdd_pull.write.format("org.apache.spark.sql.cassandra").mode("append").option("spark.cassandra.connection.host", "mastertn").options(Map("table" -> "pullsession_iqstat_state", "keyspace" -> "dqa_test_repository")).save()

    } else {
      val n = log.count().toInt
      val ts = log.collect()(n - 1)(3).toString
      val raw_table = sqlContext.read.jdbc(con, table_name, mysql_props)
      raw_table.registerTempTable("raw_table")
      val df = sqlContext.sql("select * from raw_table order by update_ts")
      val n1 = df.count().toInt
      val tsa = df.collect()(n1 - 1)(ts_col_no).toString
      if (tsa == ts) {
        print("No New Records in :" + table_name)
      } else {
        val df = raw_table.filter($"update_ts" <= tsa && $"update_ts" > ts)
        
        if(table_name == "delivery_outcome"){
                  df.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/Tracking/HP_test/raw/child_details")

        }else{
        
        df.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/Tracking/HP_test/raw/" + table_name)
        }
        //No of Records Pulled
        val State = "HP"
        val DateTime = sqlContext.sql("SELECT CURRENT_TIMESTAMP()").collect()(0)(0).toString.split(" ")
        val Date = DateTime(0)

        val PulledRecords = df.count.toInt
        val pull_data = Seq((datapullid, State, Date, table_name, PulledRecords))
        val rdd_pull = sqlContext.sparkContext.parallelize(pull_data).toDF("pullid", "state", "date", "tablename", "pulledrecords")
        rdd_pull.write.format("org.apache.spark.sql.cassandra").mode("append").option("spark.cassandra.connection.host", "mastertn").options(Map("table" -> "pullsession_iqstat_state", "keyspace" -> "dqa_test_repository")).save()

        val today_date = sqlContext.sql("SELECT CURRENT_TIMESTAMP()").collect()(0)(0).toString.split(" ")(0)
        val data = Seq(("SQL_DataLake_TN", today_date, table_name, tsa))
        val rdd = sqlContext.sparkContext.parallelize(data).toDF("dagid", "dagrundate", "tablename", "ts")
        rdd.write.format("org.apache.spark.sql.cassandra").mode("append").option("spark.cassandra.connection.host", "mastertn").options(Map("table" -> "dag_inf", "keyspace" -> "dqa_test_repository")).save()

      }
    }
  }

  def main(args: Array[String]): Unit = {

    val path = new Path("hdfs://masterHP:9000/delta/warehouse/Tracking/HP_test/raw")
    if (!fs.exists(path)) {
      fs.mkdirs(path)
    } else {
      fs.delete(path)
      fs.mkdirs(path)
    }
 
//    val DateTime = sqlContext.sql("SELECT CURRENT_TIMESTAMP()").collect()(0)(0).toString.split(" ")
//    val time = DateTime(1).split(":")
//    val datapullid = "2019-01-01" + "_" + time(0) + "-" + time(1)

    val raw_table = sqlContext.read.jdbc(con, "facility_details", mysql_props)
    if (fs.exists(new Path("hdfs://masterHP:9000/delta/warehouse/Tracking/HP_test/facility/facility_master"))) {
      raw_table.write.format("delta").mode("overwrite").save("hdfs://masterHP:9000/delta/warehouse/Tracking/HP_test/facility/facility_master")
    } else {
      raw_table.write.format("delta").save("hdfs://masterHP:9000/delta/warehouse/Tracking/HP_test/facility/facility_master")

    }
    raw_table.registerTempTable("facility")
    val df = sqlContext.sql("select * from facility order by update_ts")
    val n1 = df.count().toInt
    val tsa = df.collect()(n1 - 1)(9).toString 
    val time = tsa.split(" ")
    val datapullid = time(0) 

    

    //pulling(table_name, ts_col_no, pull_status)
    //--------- Eligible Couple -----------------
    pulling("eligible_couple", 20, datapullid)

    // -------------- Pregnant women -------------
    pulling("pregnant_women", 8, datapullid)

    // -------------- Delivery outcome ---------
    pulling("delivery_outcome", 9, datapullid)

    // ------------- child Vaccination -----------------
    pulling("child_vaccination", 9, datapullid)

    // ----------------  ASHA Master -------------
    pulling("asha_details", 9, datapullid)
    
    // ----------------  ANM Master -------------
    pulling("anm_details", 9, datapullid)

    // ---------------- Facility Master -----------
    pulling("facility_details", 9, datapullid)

    // --------------  Vaccine Master -------------
    pulling("vaccine_details", 10, datapullid)

  }
}