package com.tomtom.adeagg

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.locationtech.jts.geom._
import org.locationtech.jts.io.WKTReader
import org.locationtech.jts.operation.linemerge.LineMerger
import org.locationtech.jts.operation.polygonize.Polygonizer
import org.locationtech.jts.operation.union.UnaryUnionOp

import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable.ArrayBuffer


object Main {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .appName("Quickstart")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      ).getOrCreate()

    val isTopologicallyCorrect = udf(f = (wkt1: String, wkt2: String) => {
      val reader = new WKTReader()
      try {
        val geom1: Geometry = reader.read(wkt1)
        val geom2: Geometry = reader.read(wkt2)
        geom1.isValid && geom2.isValid && geom1.equalsTopo(geom2) && geom1.getLength == geom2.getLength && geom1.getArea == geom2.getArea
      } catch {
        case e: Exception =>
          print("Exception: " + e)
          false
      }
    })

    val createRelationGeomsUDF = udf(createRelationGeoms _)
    val df = spark.read.format("delta").load("C:\\Users\\dukare\\dev\\product_data\\relgsgeoms\\relsgeoms").filter(col("id") === "1988843832")
    val newdf = df.withColumn("relationGeoms_scala", createRelationGeomsUDF(col("inner_role_sorted"), col("outer_role_sorted"), col("tags.type")))
      .withColumn("is_equal", isTopologicallyCorrect(col("relationGeoms_scala"), col("geometry")))

//    val equal_count = newdf.filter(col("is_equal") === true).count()
    val not_equal_count = newdf.filter(col("is_equal") === false).count()
    val total_count = df.count()
    print("Total count: " + total_count
      + "\nNot equal count: " + not_equal_count)

//        .select("geometry", "relationGeoms_scala", "is_equal")
//    newdf.select("id","relationGeoms_scala","geometry").filter(col("is_equal") === false).show(100, truncate = false)

  }

  def createRelationGeoms(innerArray: Array[String], outerArray: Array[String], tag_type: String): String = {

    var innerPolygons: List[Polygon] = List()
    var innerLinearRings = Array.empty[LinearRing]
    var outerList: List[Geometry] = List()

    var unclosedInnerLineStrings = Array.empty[LineString]
    var possiblePolygonOuter: List[LineString] = List()

    val geometryFactory = new GeometryFactory()
    val wktReader = new WKTReader()

    for (line <- innerArray) {
      val geom = wktReader.read(line)
      if (geom.getGeometryType == "LineString") {
        if (!geom.asInstanceOf[LineString].isClosed) {
          unclosedInnerLineStrings :+= geom.asInstanceOf[LineString]
        } else {
          innerPolygons :+= geometryFactory.createPolygon(geom.getCoordinates)
        }
      }
      else
        innerPolygons :+= geometryFactory.createPolygon(geom.getCoordinates)
    }

    val polygonizer = new Polygonizer()
    unclosedInnerLineStrings.foreach(polygonizer.add)
    val polygons = polygonizer.getPolygons
    if (unclosedInnerLineStrings.nonEmpty && polygons.isEmpty) {
      return "I18"
    }
    polygonizer.getPolygons.foreach(polygon => innerPolygons :+= polygon.asInstanceOf[Polygon])
    innerPolygons = innerPolygons.map(polygon => if (!polygon.isValid) polygon.buffer(0).asInstanceOf[Polygon] else polygon)
    innerLinearRings = innerPolygons.map(_.getExteriorRing).toArray

    if (tag_type == "route") {

      val (lineStrings, polygons) = outerArray.map(wktReader.read).partition {
        case _: LineString => true
        case _: Polygon => false
        case _ => return "O33"
      }

      val (unclosedLineStrings, closedLineStrings) = lineStrings.collect {
        case ls: LineString => ls
      }.partition(!_.isClosed)

      val polygonBoundaries = polygons.collect {
        case p: Polygon => p.getBoundary.asInstanceOf[LineString]
      }

      val lineMerger = new LineMerger()
      unclosedLineStrings.foreach(lineMerger.add)
      val mergedLineStrings = lineMerger.getMergedLineStrings.map(_.asInstanceOf[LineString])

      val allGeometries = mergedLineStrings ++ closedLineStrings ++ polygonBoundaries

      allGeometries.size match {
        case 0 => return "O33"
        case 1 => return allGeometries.head.toString
        case _ => return new MultiLineString(allGeometries.toArray, geometryFactory).toString
      }
    }

    if (innerArray.nonEmpty && outerArray.isEmpty) {
      return createPolygonFromOnlyInners(innerArray.toList)
    }
    if (innerArray.isEmpty && outerArray.isEmpty) {
      return "I41"
    }


    for (line <- outerArray) {
      var geom: Geometry = wktReader.read(line)
      geom = geom
      if (geom.getGeometryType == "LineString") {
        if (!geom.asInstanceOf[LineString].isClosed) {
          possiblePolygonOuter :+= geom.asInstanceOf[LineString]
        }
        else if (geom.getCoordinates.length >= 4) {
          outerList :+= geometryFactory.createPolygon(geom.getCoordinates)
        }
        else {
          return "I42"
        }
      }
      else if (geom.getGeometryType == "Polygon") {
        outerList :+= geom.asInstanceOf[Polygon]
      }
      else {
        return "I43"
      }
    }

    if (outerList.length == 1 && possiblePolygonOuter.isEmpty) {
      val outerLinearRing = geometryFactory.createLinearRing(outerList.head.getCoordinates)
      val aPolygon = new Polygon(outerLinearRing, innerLinearRings, geometryFactory)
      return getValidPolygonString(aPolygon)
    }
    else if (outerList.isEmpty && possiblePolygonOuter.isEmpty && unclosedInnerLineStrings.nonEmpty) {
      return "O36"
    }
    var polygonsAll: List[Geometry] = outerList

    if (possiblePolygonOuter.nonEmpty) {
      val polygonizer = new Polygonizer()
      possiblePolygonOuter.foreach(polygonizer.add)

      val polygons = polygonizer.getPolygons.asScala
      val invalidRings = polygonizer.getInvalidRingLines

      if (polygons.isEmpty || invalidRings.nonEmpty) {
        val lineStrings = (possiblePolygonOuter ++ outerList.map(geom => geometryFactory.createLineString(geom.getCoordinates))).toArray
        return new MultiLineString(lineStrings, geometryFactory).toString
      }

      val validPolygons = polygons.collect {
        case polygon: Polygon if polygon.getExteriorRing.getCoordinates.length >= 4 => polygon
      }

      if (validPolygons.size != polygons.size) {
        return "O32"
      }

      polygonsAll ++= validPolygons
    }

    if (polygonsAll.isEmpty) {
      return "O34"
    }

    val completePolygons = getCompletePolygons(polygonsAll, innerPolygons, geometryFactory)

    if (completePolygons.length == 1) {
      return completePolygons.head.toString
    }

    val aMultiPolygon = new MultiPolygon(completePolygons.toArray, geometryFactory)
    return getValidMultiPolygonString(aMultiPolygon)

  }

  def createPolygonFromOnlyInners(memberGeoms: List[String]): String = {
    val geometryFactory = new GeometryFactory()
    val wktReader = new WKTReader(geometryFactory)
    var lineGeoms: List[Geometry] = List()
    var polyGeoms: List[Geometry] = List()
    for (entry <- memberGeoms) {
      val geom = wktReader.read(entry)
      if (geom.getGeometryType == "LineString" || geom.getGeometryType == "MultiLineString") {
        lineGeoms :+= geom
      }
      else if (geom.getGeometryType == "Polygon" || geom.getGeometryType == "MultiPolygon") {
        polyGeoms :+= geom
      }
      else {
        return "Exit1_1"
      }
    }
    val polygonizer = new Polygonizer()
    lineGeoms.foreach(polygonizer.add)
    val polygons = polygonizer.getPolygons

    var polyFromLine: List[Geometry] = List()
    for (polygon <- polygons.asScala) {
      polyFromLine :+= polygon.asInstanceOf[Polygon]
    }
    val unaryUnionOp = new UnaryUnionOp(polyFromLine.asJava)
    if (unaryUnionOp.union() == null) {
      val geometryFactory = new GeometryFactory()
      return geometryFactory.createGeometryCollection(null).toString
    }
    return unaryUnionOp.union().toString
  }

  private def getValidPolygonString(polygon: Polygon): String =
    if (polygon.isValid) polygon.toString else polygon.buffer(0).toString

  private def getValidMultiPolygonString(multiPolygon: MultiPolygon): String =
    if (multiPolygon.isValid) multiPolygon.toString else multiPolygon.buffer(0).toString


  private def getCompletePolygons(outerPolygons: List[Geometry], innerPolygons: List[Polygon], geometryFactory: GeometryFactory): ArrayBuffer[Polygon] = {
    outerPolygons.foldLeft(ArrayBuffer.empty[Polygon]) {
      case (polygonsAcc, outerPolygon) =>
        val innerLinearRingArray: Array[LinearRing] = innerPolygons
          .filter(outerPolygon.contains)
          .map(_.getExteriorRing)
          .toArray

        val outerLinearRing = outerPolygon.asInstanceOf[Polygon].getExteriorRing
        val aPolygon = new Polygon(outerLinearRing, innerLinearRingArray, geometryFactory)
        val bufferedPolygon = if (aPolygon.isValid) aPolygon else aPolygon.buffer(0)

        bufferedPolygon match {
          case p: Polygon => polygonsAcc :+ p
          case mp: MultiPolygon =>
            (0 until mp.getNumGeometries).foldLeft(polygonsAcc) { (acc, i) =>
              acc :+ mp.getGeometryN(i).asInstanceOf[Polygon]
            }
        }
    }
  }

}