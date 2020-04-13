import java.nio.charset.StandardCharsets

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.pubsub.SparkPubsubMessage

object Convert {

  val schema: StructType = StructType(
    List(
      StructField("Active", StringType, nullable = false),
      StructField("Vehicle_License_Number", StringType, nullable = false),
      StructField("Name", StringType, nullable = false),
      StructField("License_Type", StringType, nullable = false),
      StructField("Expiration_Date", StringType, nullable = false),
      StructField("Permit_License_Number", StringType, nullable = true),
      StructField("DMV_License_Plate_Number", StringType, nullable = true),
      StructField("Vehicle_VIN_Number", StringType, nullable = true),
      StructField("Wheelchair_Accessible", StringType, nullable = true),
      StructField("Certification_Date", StringType, nullable = true),
      StructField("Hack_Up_Date", StringType, nullable = true),
      StructField("Vehicle_Year", IntegerType, nullable = true),
      StructField("Base_Number", StringType, nullable = true),
      StructField("Base_Name", StringType, nullable = true),
      StructField("Base_Type", StringType, nullable = false),
      StructField("VEH", StringType, nullable = true),
      StructField("Base_Telephone_Number", StringType, nullable = true),
      StructField("Website", StringType, nullable = true),
      StructField("Base_Address", StringType, nullable = true),
      StructField("Reason", StringType, nullable = true),
      StructField("Order_Date", StringType, nullable = true),
      StructField("Last_Date_Updated", StringType, nullable = true),
      StructField("Last_Time_Updated", StringType, nullable = true)
    )
  )

  private def nullConverter(r: String) = {
    if (r.length() == 0) null else r
  }

  private def nullConverterList(r: List[String]) = {
    r.map(r => nullConverter(r))
  }

  private def convertToInt(r: String) = {
    try {
      r.toInt
    } catch {
      case _: Exception => 0
    }
  }

  def extractVehicles(input: RDD[SparkPubsubMessage]): RDD[Row] = {
    input.map(message => new String(message.getData(), StandardCharsets.UTF_8))
      .filter(_.length != 0)
      .map(_.split(""",(?=(?:[^"]*"[^"]*")*[^"]*$)"""))
      .map {
        attribute =>
          nullConverterList(attribute.take(11).toList) :::
            List(convertToInt(attribute(11))) :::
            nullConverterList(attribute.takeRight(11).toList)
      }
      .map(attribute => Row.fromSeq(attribute))
  }
}
