
import org.apache.spark.sql.{Row, SparkSession}

import org.apache.spark.sql.functions._
object Problem4 {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("CSV Reader")
      .master("local[*]") // run locally using all available cores
      .getOrCreate()

    val df = spark.read
      .option("header", "true") // first line in file contains headers
      .csv("src\\main\\resources\\train.csv")


    val jack= df.filter(df.col("Pclass").equalTo(3)  &&  df.col("Age")>18 && df.col("Age")<21 &&df.col("Sex").equalTo("male")&&df.col("SibSp").equalTo(0)&&df.col("Parch").equalTo(0)&& df.col("Survived").equalTo(0))
    jack.show()
  }
}

