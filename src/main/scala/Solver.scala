import model.{Coordinate, Solution}
import org.apache.spark.rdd.RDD


trait Solver {
  def solve(data: RDD[(String, Coordinate)]): Solution
}