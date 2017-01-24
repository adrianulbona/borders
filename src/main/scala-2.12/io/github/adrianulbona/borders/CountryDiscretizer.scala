package io.github.adrianulbona.borders

import java.io.File
import java.lang.System.currentTimeMillis

import ch.hsr.geohash.GeoHash
import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import com.vividsolutions.jts.geom.{GeometryFactory, Polygon}
import com.vividsolutions.jts.io.{WKTReader, WKTWriter}
import io.github.adrianulbona.jts.discretizer.DiscretizerFactoryImpl
import io.github.adrianulbona.jts.discretizer.util.{GeoHash2Geometry, WGS84Point2Coordinate}

import scala.collection.JavaConverters._

/**
  * Created by adrianulbona on 23/01/2017.
  */
object CountryDiscretizer {

  case class Id(continent: String, name: String, fipsCode: String, isoCode: String) {
    def toList: List[String] = List(continent, name, fipsCode, isoCode)
  }

  case class CountryBorder(id: Id, wkt: String)

  case class CountryDiscretization(country: Id, geoHashes: Set[GeoHash]) {
    def toList: List[String] = {
      country.toList :+ geoHashes.map(_.toBase32).mkString(",")
    }

    def toListWKT: List[String] = {
      country.toList :+ geoHashes2wkt(geoHashes)
    }

    def geoHashes2wkt(geoHashes: Set[GeoHash]): String = {
      val geoHash2Geometry = new GeoHash2Geometry(new WGS84Point2Coordinate)
      val factory = new GeometryFactory()
      val polygons = geoHashes
        .map(geoHash => geoHash2Geometry.apply(geoHash, factory).asInstanceOf[Polygon])
        .toArray
      new WKTWriter().write(factory.createMultiPolygon(polygons))
    }
  }

  def main(args: Array[String]): Unit = {
    write(read()
      .par
      .filter(_.id.continent == "Europe")
      .filter(_.id.name != "Russia")
      .map(discretize(5)(_))
      .toList)
  }


  def read(): List[CountryBorder] = {
    val reader = CSVReader.open(new File("data/borders_wkt.csv"))
    try {
      reader.all().map({
        case List(continent, name, fips, iso, wkt) => CountryBorder(Id(continent, name, fips, iso), wkt)
      })
    }
    finally reader.close()
  }

  def discretize(precision: Int)(border: CountryBorder): CountryDiscretization = {
    val geometry = new WKTReader().read(border.wkt)
    val refTime = currentTimeMillis()
    val geoHashes = new DiscretizerFactoryImpl()
      .discretizer(geometry)
      .apply(geometry, precision).asScala.toSet
    val duration = currentTimeMillis() - refTime
    println(s"Discretized ${border.id.name} with ${geoHashes.size} geoHashes in $duration ms.")
    CountryDiscretization(border.id, geoHashes)
  }

  def write(discretizations: List[CountryDiscretization]): Unit = {
    val writer = CSVWriter.open(new File("data/countries_discretized_wkt.csv"))
    try {
      writer.writeAll(discretizations.map(_.toListWKT))
    }
    finally writer.close()
  }
}
