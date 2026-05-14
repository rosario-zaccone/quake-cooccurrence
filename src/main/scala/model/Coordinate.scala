package model

case class Coordinate(longitude: Double, latitude: Double)

object Coordinate {
  implicit val ordering: Ordering[Coordinate] = Ordering.by(c => (c.longitude, c.latitude))
}

