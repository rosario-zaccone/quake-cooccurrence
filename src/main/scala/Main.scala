import model.{Coordinate, Solution}
import org.apache.spark.sql.SparkSession

object Main {

  /*
  export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
   */
  def main(args: Array[String]): Unit = {
    println("Prova")
    if (args.length < 2) {
      println("Usage: Main <bucket-name> <cluster-name>")
      sys.exit(1)
    }

    val bucketName = args(0)
    val clusterName = args(1)

    val spark = SparkSession.builder()
      .appName(s"RDD Terremoti - Solver1 - $clusterName")
      .getOrCreate()

    val sc = spark.sparkContext

    // Path su Google Cloud Storage
    val path = s"gs://$bucketName/dataset_trimmed.csv"

    val data = spark.read
      .option("header", "true")
      .csv(path)
      .rdd
      .map { row =>
        val date = row.getAs[String]("date").take(10)
        val latRaw = row.getAs[String]("latitude").toDouble
        val lonRaw = row.getAs[String]("longitude").toDouble

        val lat = Math.round(latRaw * 10) / 10.0
        val lon = Math.round(lonRaw * 10) / 10.0

        (date, Coordinate(lat, lon))
      }

    val solver1 = new Solver1()
    val tStart = System.nanoTime()

    val solution: Solution = solver1.solve(data)

    val elapsed = (System.nanoTime() - tStart) / 1e9

    println("==== Solver1 ====")
    println(s"Result: $solution")
    println(f"Elapsed time: $elapsed%.3f seconds")

    spark.stop()
  }
}