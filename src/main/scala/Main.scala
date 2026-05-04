import model.{Coordinate, Solution}
import org.apache.spark.sql.SparkSession

/*
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
 */


object Main {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: Main <solver-name> <bucket-name> <cluster-name> <partition-multiplier>")
      sys.exit(1)
    }

    val solverName = args(0)
    val bucketName = args(1)
    val clusterName = args(2)
    val coeff = args(3).toInt

    val spark = SparkSession.builder()
      .appName(s"RDD Terremoti - $solverName - $clusterName")
      .getOrCreate()

    val sc = spark.sparkContext


    val path = s"gs://$bucketName/dataset.csv"


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
      }.repartition(sc.defaultParallelism * coeff);


    val solver: Option[() => Any] = solverName match {
      case "Solver1" => Some(() => new Solver1().solve(data))
      case "Solver2" => Some(() => new Solver2().solve(data))
      case "Solver3" => Some(() => new Solver3().solve(data))
      case "Solver4" => Some(() => new Solver4().solve(data))
      case "Solver5" => Some(() => new Solver5().solve(data))
      case _ =>
        println(s"Unknown solver: $solverName")
        None
    }

    solver.foreach { runSolver =>
      val tStart = System.nanoTime()
      val solution: Solution = runSolver().asInstanceOf[Solution]
      val elapsed = (System.nanoTime() - tStart) / 1e9

      println(s"==== $solverName ====")
      println(s"Result: $solution")
      println(f"Elapsed time: $elapsed%.3f seconds\n")
    }

    spark.stop()
  }
}