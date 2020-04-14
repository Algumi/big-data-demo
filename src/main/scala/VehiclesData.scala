import Convert.{extractVehicles, schema}
import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.SparkPubsubMessage

object VehiclesData {

  private def countVehiclesByYear(input: DataFrame) =
    input
      .where(f"Vehicle_Year > 1800 and Vehicle_Year <= ${year(current_date())}")
      .groupBy("Vehicle_Year")
      .agg(
        count("*").as("Count_Vehicles"),
        first("Timestamp").as("Timestamp")
      )

  private def findTop3Year(input: DataFrame) =
    input
      .groupBy("Vehicle_Year")
      .agg(
        row_number().over(orderBy(count("*").desc)).as("Rank"),
        count("*").as("Count_Vehicle"),
        first("Timestamp").as("Timestamp")
      )
      .filter("Rank <= 3")

  private def countVehiclesByBaseType(input: DataFrame) =
    input
      .where("Base_Type is not NULL")
      .groupBy("Base_Type")
      .agg(
        count("*").as("Count_Vehicles"),
        first("Timestamp").as("Timestamp")
      )

  private def writeToBigquery(data: DataFrame, datasetName: String, tableName: String): Unit =
    data.write.format("bigquery").option("table", f"$datasetName.$tableName")
      .option("temporaryGcsBucket", "vehicle_metrics_bucket").mode(SaveMode.Append).save()

  def processVehiclesData(stream: DStream[SparkPubsubMessage], windowInterval: Int, slidingInterval: Int,
                             spark: SparkSession, bigQueryDataset: String): Unit = {
    stream.window(Seconds(windowInterval), Seconds(slidingInterval))
      .foreachRDD {
        rdd =>
          val vehicleDF = spark.createDataFrame(extractVehicles(rdd), schema)
            .withColumn("Timestamp", lit(date_format(current_timestamp(), "dd.MM.yyyy_hh-mm")))
            .cache()

          writeToBigquery(countVehiclesByYear(vehicleDF), bigQueryDataset, "count")
          writeToBigquery(findTop3Year(vehicleDF), bigQueryDataset, "popularity")
          writeToBigquery(countVehiclesByBaseType(vehicleDF), bigQueryDataset, "type")

          vehicleDF.write.mode(SaveMode.Append).partitionBy("timestamp")
            .parquet("gs://vehicle_test_data/app_output")
      }
  }
}
