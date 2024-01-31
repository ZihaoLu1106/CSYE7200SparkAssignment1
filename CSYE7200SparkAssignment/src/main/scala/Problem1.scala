import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

object Problem1 {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("CSV Reader")
      .master("local[*]") // run locally using all available cores
      .getOrCreate()

    val df = spark.read
      .option("header", "true") // first line in file contains headers
      .csv("src\\main\\resources\\train.csv")


    val average = df.groupBy("Pclass").agg(avg("fare"))
    average.sort(col("Pclass"))show()
  }
}
