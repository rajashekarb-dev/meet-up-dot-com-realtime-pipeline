package spark_stream_processing_app

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object main {
  def main(args: Array[String]): Unit = {
    println("***** Meetup.com rsvp stream processing application started *****")

    val kafka_topic_name = "meetuprsvptopic"
    val kafka_bootstrap_server = "localhost:9092"

    val mysql_host_name = "localhost"
    val mysql_port_no = "3306"
    val mysql_user_name = "root"
    val mysql_password = "rootuser"
    val mysql_database_name = "meetup_rsvp_db"
    val mysql_driver_class = "com.mysql.jdbc.Driver"
    val mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + mysql_port_no + "/" + mysql_database_name

    val mongodb_host_name = "localhost"
    val mongodb_port_no = "27017"
    val mongodb_user_name = "admin"
    val mongodb_password = "admin"
    val mongodb_database_name = "meetup_rsvp_db"
    val mongodb_collection_name = "meetup_rsvp_message_detail_tbl"

    // Creating spark related configurations
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Meetup.com RSVP Stream Processing Application")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Streaming meetup.com RSVP message data from Apache Kafka producer
    val meetup_rsvp_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_server)
      .option("subscribe", kafka_topic_name)
      .option("startingOffsets", "latest")
      .load()

    println("Printing Schema of transaction_detail_df: ")
    meetup_rsvp_df.writeStream.format("console").start().awaitTermination()
  }
}
