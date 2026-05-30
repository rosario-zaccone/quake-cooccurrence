import model.{Coordinate, Solution}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import java.io.PrintStream
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/*
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
 */


object Main {
  private val weakScalingFlag = "--weak-scaling"
  private val weakScalingBaseWorkers = 2

  private def saveToGCS(bucketName: String, solverName: String, clusterName: String, coeff: Int, solution: Solution, elapsed: Double): Unit = {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    val outputPath = new Path(s"gs://$bucketName/results/${solverName}_${clusterName}_coeff${coeff}_$timestamp.txt")
    val fs = FileSystem.get(outputPath.toUri, new Configuration())
    val out = new PrintStream(fs.create(outputPath, true))
    try {
      out.println(s"Solver:  $solverName")
      out.println(s"Cluster: $clusterName")
      out.println(s"Coeff:   $coeff")
      out.println(s"Pair:    (lon=${solution.pair._1.longitude}, lat=${solution.pair._1.latitude}) -> (lon=${solution.pair._2.longitude}, lat=${solution.pair._2.latitude})")
      out.println(s"Times:   ${solution.times.mkString(", ")}")
      out.println(f"Elapsed: $elapsed%.3f seconds")
    } finally { out.close(); fs.close() }
  }

  private def workerCountFromClusterName(clusterName: String): Option[Int] = {
    val WorkerCount = """.*?(\d+)w.*""".r


    clusterName match {
      case WorkerCount(workers) => Some(workers.toInt)
      case _ => None
    }
  }

  private def workerCount(spark: SparkSession, clusterName: String): Int = {
    spark.conf.getOption("spark.executor.instances")
      .flatMap(value => scala.util.Try(value.toInt).toOption)
      .orElse(workerCountFromClusterName(clusterName))
      .getOrElse(1)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println(s"Usage: Main <solver-name> <bucket-name> <cluster-name> <partition-multiplier> [$weakScalingFlag]")
      sys.exit(1)
    }

    val solverName = args(0)
    val bucketName = args(1)


    val clusterName = args(2)
    val coeff = args(3).toInt
    val weakScaling = args.drop(4).contains(weakScalingFlag)
    println(coeff)

    val spark = SparkSession.builder()
      .appName(s"RDD Terremoti - $solverName - $clusterName")
      .getOrCreate()

    val sc = spark.sparkContext


    val path = s"gs://$bucketName/dataset.csv"


    val baseData = spark.read
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

    val workers = Math.max(workerCount(spark, clusterName), weakScalingBaseWorkers)
    val scaledData =
      if (weakScaling) {
        baseData
          .zipWithIndex()
          .flatMap { case ((date, coordinate), index) =>
            val fullReplicas = workers / weakScalingBaseWorkers
            val extraReplicas = if (index % weakScalingBaseWorkers < workers % weakScalingBaseWorkers) 1 else 0
            val replicas = fullReplicas + extraReplicas
            (0 until replicas).map(replica => (s"$date-$replica", coordinate))
          }
      } else {
        baseData
      }

    val data = scaledData
      .repartition(sc.defaultParallelism * coeff);


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

      saveToGCS(bucketName, solverName, clusterName, coeff, solution, elapsed)
    }

    spark.stop()
  }


}
