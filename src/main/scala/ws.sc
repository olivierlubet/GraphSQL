
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

val spark = SparkSession
  .builder()
  .master("local")
  .appName("Spark")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

spark.stop()