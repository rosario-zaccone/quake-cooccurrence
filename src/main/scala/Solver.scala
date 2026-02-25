import model.{ Coordinate, Solution}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.math.BigDecimal.RoundingMode
//IDEA: trovo prima la coppia piu cooccorrente con numero di occorrenze, e poi filtrando le date (non gia d asubito le date)
// GK + combination
// GK + self join
// GK + (migliore fra combination e self join) + meotodo INT (trovo prima il count e poi le ate)
// RK + same di prima con le 3 opzioni
// partition?
// persist?
trait Solver {
  /*
  def getCoordPairsWithDate(coordsPerDate: RDD[(String, Seq[Coord])]):RDD[((Coord, Coord), String)] = {
    coordsPerDate.flatMap { case (date, coordsSeq) =>
      coordsSeq.combinations(2).map { combo =>
        val a = combo(0)
        val b = combo(1)
        val pair =
          if (a.longitude < b.longitude || (a.longitude == b.longitude && a.latitude <= b.latitude))
            (a, b)
          else
            (b, a)
        (pair, date)
      }
    }
  }

  def getCoordPairsWithDateSelfJoin(coordsPerDate: RDD[(String, Seq[Coord])]): RDD[((Coord, Coord), String)] = {
    val exploded: RDD[(String, Coord)] = coordsPerDate.flatMap { case (date, coordsSeq) =>
      coordsSeq.map(coord => (date, coord))
    }

    val joined: RDD[(String, (Coord, Coord))] = exploded
      .join(exploded)
      .filter { case (_, (a, b)) => a != b }
      .map { case (date, (a, b)) =>
        val pair =
          if (a.longitude < b.longitude || (a.longitude == b.longitude && a.latitude <= b.latitude))
            (a, b)
          else
            (b, a)
        (date, pair)
      }
      .distinct()
    joined.map { case (date, pair) => (pair, date) }
  }
*/
  def solve(data: RDD[(String, Coordinate)]) : Solution
}