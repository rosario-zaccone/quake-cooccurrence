import model.{Coordinate, Solution}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{RangePartitioner, SparkContext}

object LocalMain {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RDD Terremoti Co-occorrenza - Local")
      .master("local[*]")  // esegue in locale su tutti i core disponibili
      .getOrCreate()

    val filename = "./data/trimmed.csv"
    val data = spark.read
      .option("header", true)
      .csv(filename)
      .rdd
      .map { row =>
        val date = row.getAs[String]("date").take(10)
        val latRaw = row.getAs[String]("latitude").toDouble
        val lonRaw = row.getAs[String]("longitude").toDouble
        val lat = Math.round(latRaw * 10) / 10.0
        val lon = Math.round(lonRaw * 10) / 10.0
        (date, Coordinate(lat, lon))
      }
    val s1 = new Solver1();
    val sol = s1.solve(data);
    println(sol);

    spark.stop()
  }
}

// se salta tutto vai su edit confgiurations del run di localmain e metti come jdk la 11