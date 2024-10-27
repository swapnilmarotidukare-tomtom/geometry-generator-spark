package com.tomtom.adeagg

import org.locationtech.jts.geom.{Geometry, GeometryFactory, LineString, LinearRing, MultiLineString, MultiPolygon, Polygon}
import org.locationtech.jts.io.WKTReader
import org.locationtech.jts.operation.linemerge.LineMerger
import org.locationtech.jts.operation.polygonize.Polygonizer
import org.locationtech.jts.operation.union.UnaryUnionOp

import java.util
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object Main2 {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    //empty string array
    val innerArray = Array.empty[String]
    val outerArray = Array("LINESTRING (6.2720939 49.5070706, 6.2721836 49.5071691, 6.2726435 49.5075889)")
    val tag_type = "test"
    val str = createRelationGeoms(innerArray, outerArray, tag_type)
    println(str)
  }

  def createRelationGeoms(innerArray: Array[String], outerArray: Array[String], tag_type: String): String = {

    var possiblePolygonOuter: List[LineString] = List()
    val geometryFactory = new GeometryFactory()
    val wktReader = new WKTReader()

    if (tag_type == "route") {
      var entries_for_multilinestring: List[Geometry] = List()
      for (line <- outerArray) {
        val geom = wktReader.read(line)
        geom match {
          case lineString: LineString =>
            if (!lineString.isClosed) {
              possiblePolygonOuter :+= lineString
            } else {
              entries_for_multilinestring :+= geom
            }
          case polygon: Polygon =>
            entries_for_multilinestring :+= polygon.getBoundary
          case _ =>
            return "O31"
        }
      }

      if (possiblePolygonOuter.isEmpty) {
        val lineStrings: Array[LineString] = entries_for_multilinestring.map(_.asInstanceOf[LineString]).toArray
        return new MultiLineString(lineStrings, geometryFactory).toString
      }

      var lineMerger = new LineMerger()
      possiblePolygonOuter.foreach(lineMerger.add)

      var merged_line = lineMerger.getMergedLineStrings
      var merged_line_list = merged_line.asScala.toList
      val geometries = merged_line_list.map(_.asInstanceOf[LineString])
      if (geometries.length == 1 && geometries.head.isValid && entries_for_multilinestring.isEmpty) {
        return geometries.head.toString
      }
      else if (geometries.length == 1 && geometries.head.isValid && entries_for_multilinestring.nonEmpty) {
        var merged_line_n = geometries.head
        if (merged_line_n.getGeometryType == "MultiLineString") {
          //merge merged_line with entries_for_multilinestring
          val lineStrings: Array[LineString] = entries_for_multilinestring.map(_.asInstanceOf[LineString]).toArray
          val merged_line_string = merged_line_n.asInstanceOf[LineString]
          return new MultiLineString((lineStrings :+ merged_line_string), geometryFactory).toString
        }
        else {
          val lineStrings: Array[LineString] = entries_for_multilinestring.map(_.asInstanceOf[LineString]).toArray
          val geometries1: Array[LineString] = merged_line_list.map(_.asInstanceOf[LineString]).toArray
          return new MultiLineString(lineStrings ++ geometries1, geometryFactory).toString
        }
      }
      else if (geometries.length == 1 && !geometries.head.isValid) {
        return "O33"
      }
      else if (geometries.length > 1 && entries_for_multilinestring.isEmpty) {
        val geometries1: Array[LineString] = merged_line_list.map(_.asInstanceOf[LineString]).toArray
        return new MultiLineString(geometries1, geometryFactory).toString
      }
      else if (geometries.length > 1 && entries_for_multilinestring.nonEmpty) {
        val lineStrings: Array[LineString] = entries_for_multilinestring.map(_.asInstanceOf[LineString]).toArray
        val geometries1: Array[LineString] = merged_line_list.map(_.asInstanceOf[LineString]).toArray
        return new MultiLineString((geometries1 ++ lineStrings), geometryFactory).toString
      }
      else
        return "O34"
    }



    var finalInnerList: List[Geometry] = List()
    val finerOuterList = Array.empty[String]

    var innerList = Array.empty[String]
    var outerList: List[Geometry] = List()

    var innerLines: List[Geometry] = List()
    val outerLines = Array.empty[String]

    var possiblePolygonInner = Array.empty[LineString]



    for (line <- innerArray) {
      val geom = wktReader.read(line)
      if(geom.getGeometryType == "LineString" && !geom.asInstanceOf[LineString].isClosed){
        possiblePolygonInner :+= geom.asInstanceOf[LineString]
      }
      else
        innerList :+= line
    }

    if (innerList.length > 0 && possiblePolygonInner.length == 0) {
      for (inner <- innerList) {
        var polygon: Polygon = geometryFactory.createPolygon(wktReader.read(inner).getCoordinates)
        if (polygon.isValid) {
          finalInnerList :+= polygon
        } else {
          finalInnerList :+= polygon.buffer(0)
        }
      }
    } else if (innerList.length > 0 && possiblePolygonInner.length > 0) {
      val polygonizer = new Polygonizer()
      possiblePolygonInner.foreach(polygonizer.add)
      val polygons = polygonizer.getPolygons
      polygonizer.getPolygons.foreach(polygon => finalInnerList :+= polygon.asInstanceOf[Polygon])
      for (inner <- innerList) {
        var polygon: Polygon = geometryFactory.createPolygon(wktReader.read(inner).getCoordinates)
        if (polygon.isValid) {
          finalInnerList :+= polygon
        } else {
          finalInnerList :+= polygon.buffer(0)
        }
      }
    } else if (possiblePolygonInner.length > 0 && innerList.length == 0) {
      val polygonizer = new Polygonizer()
      possiblePolygonInner.foreach(polygonizer.add)
      val polygons = polygonizer.getPolygons
      polygonizer.getPolygons.foreach(polygon => finalInnerList :+= polygon.asInstanceOf[Polygon])

      if (polygonizer.getPolygons.size() == 0) {
        val lineMerge = new LineMerger()
        possiblePolygonInner.foreach(lineMerge.add)
        val mergedLines: util.Collection[_] = lineMerge.getMergedLineStrings
        val geometryList: List[Geometry] = mergedLines.asScala.toList.map(_.asInstanceOf[Geometry])
        for (line: Geometry <- geometryList) {
          if (line.isValid) {
            if (line.getGeometryType == "LineString") {
              innerLines :+= line
            } else {
              for (i <- 0 until line.getNumGeometries) {
                innerLines :+= line.getGeometryN(i)
              }
            }
          }
          else
            return "I2"
        }
      }
    }

    else if (innerList.length == 0 && possiblePolygonInner.length == 0) {
      finalInnerList = innerList.map(wktReader.read).toList
    }

    else
      return "I3"


    if (innerArray.nonEmpty && outerArray.isEmpty) {
      return createPolygonFromOnlyInners(innerArray.toList)
    }

    for (line <- outerArray) {
      var geom: Geometry = wktReader.read(line)
      geom = geom.asInstanceOf[Geometry]
      if (geom.getGeometryType == "LineString" && !geom.asInstanceOf[LineString].isClosed) {
        possiblePolygonOuter :+= geom.asInstanceOf[LineString]
      }
      else if (geom.getCoordinates.length > 4) {
        outerList :+= geom
      }
      else {
        return "I19"
      }
    }

    if (outerList.length == 1 && possiblePolygonOuter.isEmpty) {
      var innerLinearRingArray = Array.empty[LinearRing]
      for (a <- finalInnerList) {
        innerLinearRingArray :+= geometryFactory.createLinearRing(a.getCoordinates)
      }
      var outerLinearRing = geometryFactory.createLinearRing(outerList.head.getCoordinates)
      var aPolygon = new Polygon(outerLinearRing, innerLinearRingArray, geometryFactory)
      if(aPolygon.isValid){
        return aPolygon.toString
      }
      else{
        return aPolygon.buffer(0).toString
      }
    }
    else if (outerList.length > 1 && possiblePolygonOuter.isEmpty) {

      var polygons: List[Geometry] = List()
      var multis: List[MultiPolygon] = List()

      for (outer_poly <- outerList) {
        var innerLinearRingArray = Array.empty[LinearRing]
        var outerLinearRing = geometryFactory.createLinearRing(outer_poly.getCoordinates)

        for (a <- finalInnerList) {
          if (outer_poly.contains(a)) {
            innerLinearRingArray :+= geometryFactory.createLinearRing(a.getCoordinates)
          }
        }

        if (innerLinearRingArray.isEmpty) {
          polygons :+= geometryFactory.createPolygon(outerLinearRing)
        } else {
          var aPolygon = new Polygon(outerLinearRing, innerLinearRingArray, geometryFactory)
          if (aPolygon.isValid) {
            polygons :+= aPolygon
          } else {
            var afterBuffer: Geometry = aPolygon.buffer(0)
            if (afterBuffer.getGeometryType == "Polygon") {
              polygons :+= afterBuffer
            } else {
              multis :+= afterBuffer.asInstanceOf[MultiPolygon]
            }
          }
        }
      }

      if (polygons.length == 1 && multis.isEmpty) {
        return polygons.head.toString
      }


      for (multi <- multis) {
        for (i <- 0 until multi.getNumGeometries) {
          polygons :+= multi.getGeometryN(i)
        }
      }
      //convert polygons to polygon array
      var polyArray: Array[Polygon] = polygons.map(_.asInstanceOf[Polygon]).toArray
      var aMultiPolygon = new MultiPolygon(polyArray, geometryFactory)
      if (aMultiPolygon.isValid) {
        return aMultiPolygon.toString
      } else {
        return aMultiPolygon.buffer(0).toString
      }

    }

    else if (outerList.nonEmpty && possiblePolygonOuter.nonEmpty) {
      val polygonizer = new Polygonizer()
      possiblePolygonOuter.foreach(polygonizer.add)
      if (polygonizer.getPolygons.size() == 0 || polygonizer.getInvalidRingLines.size > 0) {
        var strings: Array[LineString] = Array()
        for (geom <- possiblePolygonOuter) {
          strings :+= geom.asInstanceOf[LineString]
        }
        val strings2 = outerList.map(geom => geometryFactory.createLineString(geom.getCoordinates))
        for (l <- strings2) {
          strings :+= l
        }
        return new MultiLineString(strings, geometryFactory).toString
      }

      var polygonsAll: List[Geometry] = List()
      for (outer <- outerList) {
        polygonsAll :+= outer
      }
      for (polygon <- polygonizer.getPolygons.asScala) {
        if (polygon.asInstanceOf[Polygon].getExteriorRing.getCoordinates.length >= 4) {
          polygonsAll :+= polygon.asInstanceOf[Polygon]
        }
        else {
          return "O32"
        }
      }
      var completePolygons: List[Polygon] = List()
      var multis: List[MultiPolygon] = List()
      for (polygon <- polygonsAll) {
        var innerLinearRingArray = Array.empty[LinearRing]
        for(inner_poly <- finalInnerList) {
          if (polygon.contains(inner_poly)) {
            innerLinearRingArray :+= geometryFactory.createLinearRing(inner_poly.getCoordinates)
          }
        }

        if (innerLinearRingArray.isEmpty){
          completePolygons :+= polygon.asInstanceOf[Polygon]
        }
        else {
          var aPolygon = new Polygon(polygon.asInstanceOf[Polygon].getExteriorRing, innerLinearRingArray, geometryFactory)
          if (aPolygon.isValid) {
            completePolygons :+= aPolygon
          }
          else {
            var afterBuffer: Geometry = aPolygon.buffer(0)
            if (afterBuffer.getGeometryType == "Polygon") {
              completePolygons :+= afterBuffer.asInstanceOf[Polygon]
            }
            else {
              multis :+= afterBuffer.asInstanceOf[MultiPolygon]
            }
          }
        }
      }

      if (completePolygons.length == 1 && multis.isEmpty) {
        return completePolygons.head.toString
      }

      for(multi <- multis) {
        for (i <- 0 until multi.getNumGeometries) {
          completePolygons :+= multi.getGeometryN(i).asInstanceOf[Polygon]
        }
      }

      var aMultiPolygon = new MultiPolygon(completePolygons.toArray, geometryFactory)
      if (aMultiPolygon.isValid) {
        return aMultiPolygon.toString
      }
      else {
        return aMultiPolygon.buffer(0).toString
      }
    }

    else if(possiblePolygonOuter.nonEmpty && outerList.isEmpty) {
      val polygonizer = new Polygonizer()
//      possiblePolygonOuter.foreach(polygonizer.add)
      for (line <- possiblePolygonOuter) {
        polygonizer.add(line)
      }
      if (polygonizer.getPolygons.size() == 0 || polygonizer.getInvalidRingLines.size > 0) {
        var strings: Array[LineString] = Array()
        for (geom <- possiblePolygonOuter) {
          strings :+= geom.asInstanceOf[LineString]
        }
        return new MultiLineString(strings, geometryFactory).toString
      }

      if(polygonizer.getPolygons.size() >= 1){
        var polygons : List[Geometry] = List()
        var multis : List[MultiPolygon] = List()
        for (outer_poly <- polygonizer.getPolygons.asScala){
          var outer_poly_cords = geometryFactory.createPolygon(outer_poly.asInstanceOf[Polygon].getExteriorRing.getCoordinates)
          var innerListMulPoly: List[Geometry] = List()
          for (inner_poly <- finalInnerList){
            if (outer_poly_cords.contains(inner_poly)){
              innerListMulPoly :+= inner_poly
            }
          }

          if(innerListMulPoly.isEmpty){
            polygons :+= outer_poly_cords
          }
          else{
            var aPolygon= new Polygon(outer_poly_cords.getExteriorRing, innerListMulPoly.map(_.asInstanceOf[Polygon].getExteriorRing).toArray, geometryFactory)
            if(aPolygon.isValid){
              polygons :+= aPolygon
            }
            else{
              var afterBuffer: Geometry = aPolygon.buffer(0)
              if(afterBuffer.getGeometryType == "Polygon"){
                polygons :+= afterBuffer
              }
              else{
                multis :+= afterBuffer.asInstanceOf[MultiPolygon]
              }
            }
          }

        }

        if (polygons.length == 1 && multis.isEmpty){
          return polygons.head.toString
        }

        for(multi <- multis){
          for (i <- 0 until multi.getNumGeometries){
            polygons :+= multi.getGeometryN(i)
          }
        }
        var aMultiPolygon= new MultiPolygon(polygons.map(_.asInstanceOf[Polygon]).toArray, geometryFactory)
        if(aMultiPolygon.isValid){
          return aMultiPolygon.toString
        }
        else{
          return aMultiPolygon.buffer(0).toString
        }
      }
      else{
        var linerMerge = new LineMerger()
        possiblePolygonOuter.foreach(linerMerge.add)
        if(linerMerge.getMergedLineStrings.size() ==1 && linerMerge.getMergedLineStrings.head.asInstanceOf[LineString].isValid){
          return linerMerge.getMergedLineStrings.head.toString
        }
        else{
          return "O35"
        }
      }
    }

    else if(outerList.isEmpty && possiblePolygonOuter.isEmpty && possiblePolygonInner.nonEmpty) {
      return "O36"
    }
    else {
      return "O37"
    }
    return "Heeeey!"
  }

    def createPolygonFromOnlyInners(memberGeoms: List[String]): String = {
      val geometryFactory = new GeometryFactory()
      val wktReader = new WKTReader(geometryFactory)
      var lineGeoms: List[Geometry] = List()
      var polyGeoms: List[Geometry] = List()
      for (entry <- memberGeoms) {
        val geom = wktReader.read(entry)
        geom match {
          case lineString: LineString =>
            lineGeoms :+= lineString
          case polygon: Polygon =>
            polyGeoms :+= polygon
          case _ =>
            return "Exit2_1"
        }
        val polygonizer = new Polygonizer()
        lineGeoms.foreach(polygonizer.add)
        val polygons = polygonizer.getPolygons

        var polyFromLine: List[Geometry] = List()
        for (polygon <- polygons.asScala) {
          polyFromLine :+= polygon.asInstanceOf[Polygon]
        }
        var unaryUnionOp = new UnaryUnionOp(polyFromLine.asJava)
        return unaryUnionOp.union().toString

      }

      return "Hello"
    }

  }