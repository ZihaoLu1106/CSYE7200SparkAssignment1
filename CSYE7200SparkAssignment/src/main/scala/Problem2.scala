
import org.apache.spark.sql.{Row, SparkSession}

import org.apache.spark.sql.functions._
object Problem2 {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("CSV Reader")
      .master("local[*]") // run locally using all available cores
      .getOrCreate()

    val df = spark.read
      .option("header", "true") // first line in file contains headers
      .csv("src\\main\\resources\\train.csv")


    val survive= df.groupBy("Pclass").agg(avg("Survived")).alias("surviver percentage")
    survive.sort(col("Pclass"))show()
  }
}
