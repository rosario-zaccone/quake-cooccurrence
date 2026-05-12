import model.{Coordinate, Solution}
import org.apache.spark.rdd.RDD


class Solver1 extends Solver {
  override def solve(data: RDD[(String, Coordinate)]): Solution = {

    val grouped: RDD[(String, Set[Coordinate])] =
      data
        .groupByKey()
        .mapValues(_.toSet)

    val pairsWithDate: RDD[((Coordinate, Coordinate), String)] = grouped.flatMap { case (date, coordsSet) =>
      val coordsSeq = coordsSet.toSeq
      for {
        i <- coordsSeq.indices
        j <- (i + 1) until coordsSeq.length
      } yield {
        val a = coordsSeq(i)
        val b = coordsSeq(j)
        val orderedPair =
          if (a.longitude < b.longitude || (a.longitude == b.longitude && a.latitude <= b.latitude)) (a, b)
          else (b, a)
        (orderedPair, date)
      }
    }

    val groupedByPair: RDD[((Coordinate, Coordinate), Iterable[String])] = pairsWithDate.groupByKey()

    val pairStats: RDD[((Coordinate, Coordinate), (Int, Seq[String]))] =
      groupedByPair.mapValues(dates => (dates.size, dates.toList.sorted))

    val maxPair = pairStats.reduce { (a, b) =>
      if (a._2._1 >= b._2._1) a else b
    }

    Solution(
      pair = maxPair._1,
      times = maxPair._2._2
    )
  }
}


class Solver2 extends Solver {
  override def solve(data: RDD[(String, Coordinate)]): Solution = {
    val grouped: RDD[(String, Set[Coordinate])] =
      data.aggregateByKey(Set.empty[Coordinate])(
        (set, coord) => set + coord,
        (s1, s2) => s1 ++ s2
      )

    val pairsWithDate: RDD[((Coordinate, Coordinate), String)] = grouped.flatMap { case (date, coordsSet) =>
      val coordsSeq = coordsSet.toSeq
      for {
        i <- coordsSeq.indices
        j <- (i + 1) until coordsSeq.length
      } yield {
        val a = coordsSeq(i)
        val b = coordsSeq(j)
        val orderedPair =
          if (a.longitude < b.longitude || (a.longitude == b.longitude && a.latitude <= b.latitude)) (a, b)
          else (b, a)
        (orderedPair, date)
      }
    }

    val groupedByPair: RDD[((Coordinate, Coordinate), Iterable[String])] =
      pairsWithDate.groupByKey()

    val pairStats: RDD[((Coordinate, Coordinate), (Int, Seq[String]))] =
      groupedByPair.mapValues(dates => (dates.size, dates.toList.sorted))

    val maxPair = pairStats.reduce { (a, b) =>
      if (a._2._1 >= b._2._1) a else b
    }

    Solution(
      pair = maxPair._1,
      times = maxPair._2._2
    )
  }
}


class Solver3 extends Solver {
  override def solve(data: RDD[(String, Coordinate)]): Solution = {
    val grouped: RDD[(String, Set[Coordinate])] =
      data.aggregateByKey(Set.empty[Coordinate])(
        (set, coord) => set + coord,
        (s1, s2) => s1 ++ s2
      )

    val pairsWithDate: RDD[((Coordinate, Coordinate), String)] = grouped.flatMap { case (date, coordsSet) =>
      coordsSet.toSeq.combinations(2).map { case Seq(a, b) =>
        val orderedPair =
          if (a.longitude < b.longitude || (a.longitude == b.longitude && a.latitude <= b.latitude)) (a, b)
          else (b, a)
        (orderedPair, date)
      }
    }

    val groupedByPair: RDD[((Coordinate, Coordinate), Iterable[String])] =
      pairsWithDate.groupByKey()

    val pairStats: RDD[((Coordinate, Coordinate), (Int, Seq[String]))] =
      groupedByPair.mapValues(dates => (dates.size, dates.toList.sorted))

    val maxPair = pairStats.reduce { (a, b) =>
      if (a._2._1 >= b._2._1) a else b
    }

    Solution(
      pair = maxPair._1,
      times = maxPair._2._2
    )
  }
}


class Solver4 extends Solver {
  def solve(data: RDD[(String, Coordinate)]): Solution = {

    val coordsByDate: RDD[(String, Set[Coordinate])] =
      data.aggregateByKey(Set.empty[Coordinate])(
        (set, coord) => set + coord,
        (s1, s2) => s1 ++ s2
      )

    val pairs: RDD[((Coordinate, Coordinate), String)] =
      coordsByDate.flatMap { case (date, coordsSet) =>
        val coordsSeq = coordsSet.toSeq
        val n = coordsSeq.length
        val buffer = scala.collection.mutable.ArrayBuffer.empty[((Coordinate, Coordinate), String)]
        var i = 0
        while (i < n) {
          var j = i + 1
          while (j < n) {
            val a = coordsSeq(i)
            val b = coordsSeq(j)

            val pair =
              if (a.longitude < b.longitude ||
                (a.longitude == b.longitude && a.latitude <= b.latitude))
                (a, b)
              else
                (b, a)

            buffer += ((pair, date))
            j += 1
          }
          i += 1
        }
        buffer
      }

    val pairDates: RDD[((Coordinate, Coordinate), Set[String])] =
      pairs.aggregateByKey(Set.empty[String])(
        seqOp  = (set, date) => set + date,
        combOp = (s1, s2)   => s1 | s2
      )

    val best = pairDates.reduce { (a, b) =>
      if (a._2.size >= b._2.size) a else b
    }

    Solution(pair = best._1, times = best._2.toSeq.sorted)
  }
}


class Solver5 extends Solver {
  override def solve(data: RDD[(String, Coordinate)]): Solution = {

    val coordsByDate: RDD[(String, Set[Coordinate])] =
      data.aggregateByKey(Set.empty[Coordinate])(
        (set, coord) => set + coord,
        (s1, s2) => s1 ++ s2
      )

    val pairsWithDate: RDD[((Coordinate, Coordinate), String)] =
      coordsByDate.mapPartitions { iter =>
        iter.flatMap { case (date, coordsSet) =>
          val coordsSeq = coordsSet.toSeq
          val n = coordsSeq.length
          val buffer = scala.collection.mutable.ArrayBuffer
            .empty[((Coordinate, Coordinate), String)]
          var i = 0
          while (i < n) {
            var j = i + 1
            while (j < n) {
              val a = coordsSeq(i)
              val b = coordsSeq(j)
              val pair =
                if (a.longitude < b.longitude ||
                  (a.longitude == b.longitude && a.latitude <= b.latitude))
                  (a, b) else (b, a)
              buffer += ((pair, date))
              j += 1
            }
            i += 1
          }
          buffer
        }
      }


    val pairDates: RDD[((Coordinate, Coordinate), Set[String])] =
      pairsWithDate.aggregateByKey(Set.empty[String])(
        (set, date) => set + date,
        (s1, s2) => s1 | s2
      )


    val best = pairDates.treeReduce { (a, b) =>
      if (a._2.size >= b._2.size) a else b
    }

    Solution(
      pair  = best._1,
      times = best._2.toSeq.sorted
    )
  }
}
