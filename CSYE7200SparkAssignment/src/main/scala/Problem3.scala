
import org.apache.spark.sql.{Row, SparkSession}

import org.apache.spark.sql.functions._
object Problem3 {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("CSV Reader")
      .master("local[*]") // run locally using all available cores
      .getOrCreate()

    val df = spark.read
      .option("header", "true") // first line in file contains headers
      .csv("src\\main\\resources\\train.csv")


    val rose= df.filter(df.col("Pclass").equalTo(1)  &&  df.col("Age").equalTo(17) && df.col("Sex").equalTo("female") &&df.col("SibSp").equalTo(0)&&df.col("Parch").equalTo(0))

    rose.show()
  }
}

