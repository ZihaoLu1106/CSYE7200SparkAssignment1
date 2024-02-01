
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


    val survive= df.groupBy("Pclass").agg(avg("Survived").alias("survived rate"))
    survive.sort(col("Pclass")).show()


    val maxAvgSurvived = survive.agg(max("survived rate")).head().getDouble(0)
    val maxAvgSurvivedRow = survive.filter(col("survived rate") === maxAvgSurvived)
    val maxClass= maxAvgSurvivedRow.select("Pclass").head()(0)
    val maxRate= maxAvgSurvivedRow.select("survived rate").head()(0)
    println("The class has the highest survival rate is "+maxClass+" ,which has "+maxRate+" rate to survive")


  }
}
