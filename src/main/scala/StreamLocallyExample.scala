import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession

object StreamLocallyExample {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder.master("Example")
      .appName("StreamLocallyExample")
      .getOrCreate()


    import spark.implicits._
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()


    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
