
import org.apache.spark.sql.{Row, SparkSession}

import org.apache.spark.sql.functions._
object Problem5 {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("CSV Reader")
      .master("local[*]") // run locally using all available cores
      .getOrCreate()

    val df = spark.read
      .option("header", "true") // first line in file contains headers
      .csv("src\\main\\resources\\train.csv")


    val dfWithMark = df.withColumn("Age Range", ceil(lit(floor(df.col("Age"))/10)))
    val ageRateFare=dfWithMark.filter(col("Age Range").isNotNull).groupBy("Age Range").agg(avg("Fare"))
    ageRateFare.sort(col("Age Range")).show()
    val ageRateSurvived= dfWithMark.filter(col("Age Range").isNotNull).groupBy("Age Range").agg(avg("Survived"))
    ageRateSurvived.sort(col("Age Range")).show()
  }
}

