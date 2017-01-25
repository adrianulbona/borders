package io.github.adrianulbona.borders

import java.io.File
import java.lang.System.currentTimeMillis

import ch.hsr.geohash.GeoHash
import com.github.tototoshi.csv.CSVReader
import com.github.tototoshi.csv.CSVWriter.open
import com.vividsolutions.jts.io.WKTReader
import io.github.adrianulbona.jts.discretizer.DiscretizerFactoryImpl

import scala.collection.JavaConverters._

/**
  * Created by adrianulbona on 23/01/2017.
  */
object CountryDiscretizer {

  case class Id(continent: String, name: String, fipsCode: String, isoCode: String) {
    def toList: List[String] = List(continent, name, fipsCode, isoCode)
  }

  case class Border(id: Id, wkt: String)

  case class Surface(country: Id, geoHashes: Set[GeoHash]) {
    def toList: List[String] = country.toList :+ geoHashes.map(_.toBase32).mkString(",")
  }

  def main(args: Array[String]): Unit = {
    val precision = 5
    write(read()
      .par
      .map(discretize(precision)(_))
      .toList)
  }

  def read(): List[Border] = {
    val reader = CSVReader.open(new File("data/borders_wkt.csv"))
    try {
      reader.all().map({
        case List(continent, name, fips, iso, wkt) => Border(Id(continent, name, fips, iso), wkt)
      })
    }
    finally reader.close()
  }

  def discretize(precision: Int)(border: Border): Surface = {
    val geometry = new WKTReader().read(border.wkt)
    val refTime = currentTimeMillis()
    val geoHashes = new DiscretizerFactoryImpl()
      .discretizer(geometry)
      .apply(geometry, precision).asScala.toSet
    val duration = currentTimeMillis() - refTime
    println(s"Discretized ${border.id.name} with ${geoHashes.size} geoHashes in $duration ms.")
    Surface(border.id, geoHashes)
  }

  def write(discretizations: List[Surface]): Unit = {
    val writerGeoHashes = open(new File(s"data/countries_discretized.csv"))
    try
      writerGeoHashes.writeAll(discretizations.map(_.toList))
    finally writerGeoHashes.close()
  }
}
