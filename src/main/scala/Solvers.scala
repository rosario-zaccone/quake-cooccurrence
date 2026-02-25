import model.{Coordinate, Solution}
import org.apache.spark.rdd.RDD


//groubykey + combinations
class Solver1 extends Solver {
  override def solve(data: RDD[(String, Coordinate)]): Solution = {

    val grouped: RDD[(String, Set[Coordinate])] =
      data
        .groupByKey()
        .mapValues(_.toSet)

    val pairsWithDate: RDD[((Coordinate, Coordinate), String)] = grouped.flatMap { case (date, coordsSet) =>
      coordsSet.toSeq.combinations(2).map { case Seq(a, b) =>
        // Ordina la coppia per evitare duplicati (A,B) e (B,A)
        val orderedPair =
          if (a.longitude < b.longitude || (a.longitude == b.longitude && a.latitude <= b.latitude))
            (a, b)
          else
            (b, a)

        (orderedPair, date)
      }
    }

    val groupedByPair: RDD[((Coordinate, Coordinate), Iterable[String])] = pairsWithDate.groupByKey()

    val pairStats: RDD[((Coordinate, Coordinate), (Int, Seq[String]))] = groupedByPair.mapValues { dates =>
      val uniqueDates = dates.toSet
      (uniqueDates.size, uniqueDates.toSeq.sorted)
    }

    val maxPair = pairStats.max()(Ordering.by(_._2._1))

    return Solution(
      pair = maxPair._1,
      times = maxPair._2._2
    )

  }
}

//groupbykey + sjoin
class Solver2 extends Solver {
  override def solve(data: RDD[(String, Coordinate)]): Solution = {

    val grouped: RDD[(String, Set[Coordinate])] =
      data
        .groupByKey()
        .mapValues(_.toSet)

    val exploded: RDD[(String, Coordinate)] =
      grouped.flatMap { case (date, coords) =>
        coords.map(c => (date, c))
      }

    val pairsWithDate: RDD[((Coordinate, Coordinate), String)] =
      exploded
        .join(exploded)
        .filter { case (_, (a, b)) =>
          a != b &&
            (a.longitude < b.longitude ||
              (a.longitude == b.longitude && a.latitude <= b.latitude))
        }
        .map { case (date, (a, b)) =>
          ((a, b), date)
        }

    val groupedByPair: RDD[((Coordinate, Coordinate), Iterable[String])] =
      pairsWithDate.groupByKey()

    val pairStats: RDD[((Coordinate, Coordinate), (Int, Seq[String]))] =
      groupedByPair.mapValues { dates =>
        val uniqueDates = dates.toSet
        (uniqueDates.size, uniqueDates.toSeq.sorted)
      }

    val maxPair = pairStats.max()(Ordering.by(_._2._1))

    Solution(
      pair = maxPair._1,
      times = maxPair._2._2
    )
  }
}

//groupbykey, count instead of groupby
class Solver3 extends Solver {
  override def solve(data: RDD[(String, Coordinate)]): Solution = {

    val grouped: RDD[(String, Set[Coordinate])] =
      data
        .groupByKey()
        .mapValues(_.toSet)

    val pairsWithDate: RDD[((Coordinate, Coordinate), String)] = grouped.flatMap { case (date, coordsSet) =>
      coordsSet.toSeq.combinations(2).map { case Seq(a, b) =>

        val orderedPair =
          if (a.longitude < b.longitude || (a.longitude == b.longitude && a.latitude <= b.latitude))
            (a, b)
          else
            (b, a)

        (orderedPair, date)
      }
    }

    val pairWithCount: RDD[((Coordinate, Coordinate), Int)] = pairsWithDate.map{case(coordPair, _) => (coordPair, 1)}.reduceByKey((a, b) => a + b)
    val maxPair = pairWithCount.reduce {
      case ((coordPairA, countA), (coordPairB, countB)) =>
        if (countA >= countB) (coordPairA, countA)
        else (coordPairB, countB)
    }

    val dates = pairsWithDate.filter{case(coord, _) => coord == maxPair._1}
      .map{case(coordPair, dates) => dates}
      .collect()

    return Solution(
      pair = maxPair._1,
      times = dates
    )

  }
}

/*


/***********************************************
 *
 * REDUCEBYKEY
 **************************************************/
class SolverRD1(sc: SparkContext, path: String) extends Solver(sc, path) {
  override def solve(): Solution = {
    val coordDateRdd: RDD[(Coord, String)] = rddFromCsv()

    val coordsPerDate: RDD[(String, Seq[Coord])] = coordDateRdd
      .map { case (coord, date) => (date, Set(coord)) }
      .reduceByKey(_ ++ _)
      .mapValues(_.toSeq)

    val coordPairsWithDate: RDD[((Coord, Coord), String)] = getCoordPairsWithDate(coordsPerDate)

    val pairCounts: RDD[((Coord, Coord), Int)] = coordPairsWithDate
      .map(pairWithDate => (pairWithDate._1, 1))
      .reduceByKey(_ + _)

    val maxPairTuple = pairCounts.reduce { (a, b) => if (a._2 >= b._2) a else b }
    val maxCoordPair = maxPairTuple._1

    val maxDates: Array[String] = coordPairsWithDate
      .filter(_._1 == maxCoordPair)
      .map(_._2)
      .distinct()
      .collect()
      .sorted

    Solution(pair = maxCoordPair, times = maxDates.toSeq)
  }
}

class SolverRD2(sc: SparkContext, path: String) extends Solver(sc, path) {
  override def solve(): Solution = {
    val coordDateRdd: RDD[(Coord, String)] = rddFromCsv()

    val coordsPerDate: RDD[(String, Seq[Coord])] = coordDateRdd
      .map { case (coord, date) => (date, Set(coord)) }
      .reduceByKey(_ ++ _)
      .mapValues(_.toSeq)
    println("A1")

    val coordPairsWithDate: RDD[((Coord, Coord), String)] = getCoordPairsWithDate(coordsPerDate)
    println("A2")
    val pairWithOne: RDD[((Coord, Coord), Int)] = coordPairsWithDate.map(pairWithDate => (pairWithDate._1, 1))
    println("A3")
    val pairCounts: RDD[((Coord, Coord), Int)] = pairWithOne
      .partitionBy(new RangePartitioner(sc.defaultParallelism, pairWithOne))
      .reduceByKey(_ + _)
    println("A4")
    val maxPairTuple = pairCounts.reduce { (a, b) => if (a._2 >= b._2) a else b }
    val maxCoordPair = maxPairTuple._1
    println("A5")
    val maxDates: Array[String] = coordPairsWithDate
      .filter(_._1 == maxCoordPair)
      .map(_._2)
      .distinct()
      .collect()
      .sorted

    Solution(pair = maxCoordPair, times = maxDates.toSeq)
  }
}
*/