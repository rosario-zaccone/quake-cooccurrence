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

  private def replicateForWeakScaling(
                                       data: org.apache.spark.rdd.RDD[(String, Coordinate)],
                                       workers: Int
                                     ): org.apache.spark.rdd.RDD[(String, Coordinate)] = {
    require(
      workers >= weakScalingBaseWorkers,
      s"Weak scaling requires at least $weakScalingBaseWorkers workers, found $workers"
    )

    require(
      workers % weakScalingBaseWorkers == 0,
      s"Weak scaling requires workers to be a multiple of $weakScalingBaseWorkers, found $workers"
    )

    val replicaFactor = workers / weakScalingBaseWorkers

    data.flatMap { case (date, coordinate) =>
      (0 until replicaFactor).map { replica =>
        val scaledDate =
          if (replicaFactor == 1) date
          else f"$date-replica-$replica%02d"

        val scaledCoordinate =
          if (replica == 0) {
            coordinate
          } else {
            val offset = replica * 1000.0
            Coordinate(
              coordinate.longitude + offset,
              coordinate.latitude + offset
            )
          }

        (scaledDate, scaledCoordinate)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      println(s"Usage: Main <solver-name> <bucket-name> <cluster-name> <partition-multiplier> <workers> [$weakScalingFlag]")
      sys.exit(1)
    }

    val solverName = args(0)
    val bucketName = args(1)


    val clusterName = args(2)
    val coeff = args(3).toInt
    val workers = args(4).toInt
    val weakScaling = args.drop(5).contains(weakScalingFlag)

    if (workers <= 0) {
      println(s"Invalid worker count: $workers")
      sys.exit(1)
    }

    println(s"Partition multiplier: $coeff")
    println(s"Workers: $workers")
    println(s"Weak scaling: $weakScaling")

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

        (date, Coordinate(lon, lat))
      }

    val scaledData =
      if (weakScaling) {
        println(s"[DEBUG] Weak scaling enabled: replicating dataset for $workers workers")
        replicateForWeakScaling(baseData, workers)
      } else {
        println("[DEBUG] Weak scaling disabled: using original dataset without replication")
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
