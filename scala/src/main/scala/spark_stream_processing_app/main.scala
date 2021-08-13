package spark_stream_processing_app

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import java.util.Properties

object main {
  def main(args: Array[String]): Unit = {
    println("***** Meetup.com rsvp stream processing application started *****")

    val kafka_topic_name = "meetuprsvptopic"
    val kafka_bootstrap_server = "localhost:9092"

    val mysql_host_name = "localhost"
    val mysql_port_no = "3306"
    val mysql_user_name = "root"
    val mysql_password = "rootuser"
    val mysql_database_name = "meetup_rsvp_database"
    val mysql_driver_class = "com.mysql.jdbc.Driver"
    val mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + mysql_port_no + "/" + mysql_database_name + "?characterEncoding=utf8"
    val mysql_table_name = "meetup_rsvp_message_agg_detail_tbl"

    val mongodb_host_name = "127.0.0.1"
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

  //      println("Printing Schema of transaction_detail_df: ")
//    meetup_rsvp_df.writeStream.format("console").start().awaitTermination()

    // Defining schema
    val meetup_rsvp_message_schema = StructType(Array(
      StructField("venue", StructType(Array(
        StructField("venue_name", StringType),
        StructField("lon", StringType),
        StructField("lat", StringType),
        StructField("venue_id", StringType),
      ))),
      StructField("visibility", StringType),
      StructField("response", StringType),
      StructField("guests", StringType),
      StructField("member", StructType(Array(
        StructField("member_id", StringType),
        StructField("photo", StringType),
        StructField("member_name", StringType)
      ))),
      StructField("rsvp_id", StringType),
      StructField("mtime", StringType),
      StructField("event", StructType(Array(
        StructField("event_name", StringType),
        StructField("event_id", StringType),
        StructField("time", StringType),
        StructField("event_url", StringType)
      ))),
      StructField("group", StructType(Array(
        StructField("group_topics", ArrayType(StructType(Array(
          StructField("urlkey", StringType),
          StructField("topic_name", StringType)
        )))),
        StructField("group_city", StringType),
        StructField("group_country", StringType),
        StructField("group_id", StringType),
        StructField("group_name", StringType),
        StructField("group_lon", StringType),
        StructField("group_urlname", StringType),
        StructField("group_state", StringType),
        StructField("group_lat", StringType)
      )))
    ))

    val meetup_rsvp_df_1 = meetup_rsvp_df.selectExpr("CAST(value as STRING)", "CAST(timestamp as TIMESTAMP)")

    val meetup_rsvp_df_2 = meetup_rsvp_df_1.select(from_json(col("value"), meetup_rsvp_message_schema).as("message_detail"), col("timestamp"))

    val meetup_rsvp_df_3 = meetup_rsvp_df_2.select("message_detail.*", "timestamp")

    val meetup_rsvp_df_4 = meetup_rsvp_df_3.select(col("group.group_name"),
      col("group.group_country"), col("group.group_state"), col("group.group_city"),
      col("group.group_lat"), col("group.group_lon"), col("group.group_id"),
      col("group.group_topics"), col("member.member_name"), col("response"),
      col("guests"), col("venue.venue_name"), col("venue.lon"), col("venue.lat"),
      col("venue.venue_id"), col("visibility"), col("member.member_id"),
      col("member.member_name"), col("event.event_name"), col("event.event_id"),
      col("event.time"), col("event.event_url"))

    println("Printing Schema of meetup rsvp revised: ")
    meetup_rsvp_df_4.printSchema()

//    println("Writing stream to console")
//    meetup_rsvp_df_4.writeStream.format("console").start().awaitTermination()

    val spark_mongodb_output_uri = "mongodb://" + mongodb_host_name + ":" + mongodb_port_no + "/" + mongodb_database_name + "." + mongodb_collection_name

    println("***** Saving spark stream to " + spark_mongodb_output_uri + " *****")

  // Writing Meetup RSVP Dataframe into MongoDB Collection Starts Here
    val mongo_query = meetup_rsvp_df_4.writeStream
    .trigger(Trigger.ProcessingTime("20 seconds"))
    .outputMode("update")
    .foreachBatch{ (batchDF: DataFrame, batchId: Long) =>
      val batchDataFrame = batchDF.withColumn("batch_id", lit(batchId))

      println("Saving Batch ID: " + lit(batchId) + " to MongoDB")
      // Transform batchDF and write it to sink/target/persistent storage
      // Write data from spark dataframe to database
      batchDataFrame.write
        .format("mongo")
        .mode("append")
        .option("uri", spark_mongodb_output_uri)
        .option("database", mongodb_database_name)
        .option("collection", mongodb_collection_name)
        .save()
    }.start()

//    query.awaitTermination()

    // Aggregation - find response_count bby grouping group_name,
    // group_country, group_state, group_city, group_lat, group_lon, response
    val meetup_rsvp_df_aggregated = meetup_rsvp_df_4.groupBy("group_name", "group_country",
    "group_state", "group_city", "group_lat", "group_lon", "response")
      .agg(count(col("response")).as("response_count"))

    println("Schema of aggregated data")
    meetup_rsvp_df_aggregated.printSchema()

    println("***** Saving spark stream to " + mysql_jdbc_url + " *****")

    // Writing aggregated response into console for debugging purpose
//    val meetup_rsvp_aggregated_transaction = meetup_rsvp_df_aggregated.writeStream.format("console").outputMode("update").option("truncate", "false").start()

    val mysql_properties = new Properties()
    mysql_properties.put("driver", mysql_driver_class)
    mysql_properties.put("user", mysql_user_name)
    mysql_properties.put("password", mysql_password)

    // Writing aggregated data frame to MySQL DB
    meetup_rsvp_df_aggregated.writeStream
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .outputMode("update")
      .foreachBatch{ (batchDF: DataFrame, batchId: Long) =>
        val batchDataFrame = batchDF.withColumn("batch_id", lit(batchId))

        println("Saving Batch ID: " + lit(batchId) + " to MySQL")

        // Transform batchDF and write it to sink/target/persistent storage
        // Write data from spark dataframe to database
        foreach_batch_function(batchDataFrame, batchId, mysql_jdbc_url, mysql_database_name, mysql_table_name, mysql_properties)
      }.start().awaitTermination()

//    sql_query.awaitTermination()
//    meetup_rsvp_aggregated_transaction.awaitTermination();

  }

  def foreach_batch_function(df: DataFrame, epoch_id: Long, mysql_jdbc_url: String, mysql_database_name: String, mysql_table_name: String, mysql_properties: Properties): Unit = {
    df.write
      .mode("append")
      .jdbc(mysql_jdbc_url, mysql_table_name, mysql_properties)
  }
}
