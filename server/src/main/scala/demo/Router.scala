package demo

import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{ContentType, HttpEntity, HttpResponse, MediaTypes}
import akka.http.scaladsl.unmarshalling.Unmarshaller._
import ch.megard.akka.http.cors.CorsDirectives._
import com.typesafe.config._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.interpolation._
import geotrellis.raster.render._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json._
import org.apache.spark.SparkContext
import spray.json._
import java.lang.management.ManagementFactory
import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}

import geotrellis.raster.io.geotiff.GeoTiff
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, DateTimeZone}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

// Actor that routes incoming messages to outbound actors.
// The router routes the messages sent to it to its underlying actors called 'routees'.
class Router(readerSet: ReaderSet, sc: SparkContext) extends Directives with AkkaSystem.LoggerExecutor {
  import scala.concurrent.ExecutionContext.Implicits.global
  val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ")
  val metadataReader = readerSet.metadataReader
  val attributeStore = readerSet.attributeStore

  val ndviColorMap =
    ColorMap.fromStringDouble("0.05:ffffe5aa;0.1:f7fcb9ff;0.2:d9f0a3ff;0.3:addd8eff;0.4:78c679ff;0.5:41ab5dff;0.6:238443ff;0.7:006837ff;1:004529ff").get

  def pngAsHttpResponse(png: Png): HttpResponse =
    HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`image/png`), png.bytes))

  def timedCreate[T](f: => T): (T, String) = { // Type inference
    val s = System.currentTimeMillis
    val result = f
    val e = System.currentTimeMillis
    val t = "%,d".format(e - s)
    result -> t
  }


  def isLandsat(name: String) =
    name.contains("landsat")

  // Route chaining: tries a second route if a given first one was rejected (concatenation operator ~ becomes available via Directives)
  def routes =
    path("ping") { complete { "pong\n" } } ~
      path("catalog") { catalogRoute }  ~
      pathPrefix("tiles") { tilesRoute } ~
      pathPrefix("diff") { diffRoute } ~
      pathPrefix("mean") { polygonalMeanRoute } ~
      pathPrefix("series") { timeseriesRoute } ~
      pathPrefix("readall") { readallRoute }

  /** Return a JSON representation of the catalog */
  def catalogRoute =
    cors() {
      get { // GET !
        import spray.json.DefaultJsonProtocol._
        complete {
          Future {
            println("\nIn catalogRoute... (GET)\n")

            val layerInfo =
              metadataReader.layerNamesToZooms //Map[String, Array[Int]]
                .keys
                .toList
                .sorted
                .map { name =>
                  // assemble catalog from metadata common to all zoom levels
                  val extent = {
                    val (extent, crs) = Try {
                      attributeStore.read[(Extent, CRS)](LayerId(name, 0), "extent")
                    }.getOrElse((LatLng.worldExtent, LatLng))

                    extent.reproject(crs, LatLng)
                  }

                  val times = attributeStore.read[Array[Long]](LayerId(name, 0), "times")
                    .map { instant =>
                      dateTimeFormat.format(ZonedDateTime.ofInstant(instant, ZoneOffset.ofHours(-4)))
                    }
                  (name, extent, times.sorted)
                }

            JsObject(
              "layers" ->
                layerInfo.map { li =>
                  val (name, extent, times) = li
                  JsObject(
                    "name" -> JsString(name),
                    "extent" -> JsArray(Vector(Vector(extent.xmin, extent.ymin).toJson, Vector(extent.xmax, extent.ymax).toJson)),
                    "times" -> times.toJson,
                    "isLandsat" -> JsBoolean(true)
                  )
                }.toJson
            ) // end JsObject
          } // end Future
        }
      } // end GET
    }

  /** Find the breaks for one layer */
  def tilesRoute =
    pathPrefix(Segment / IntNumber / IntNumber / IntNumber) { (layer, zoom, x, y) =>
      parameters('time, 'operation ?) { (timeString, operationOpt) =>
        val time = ZonedDateTime.parse(timeString, dateTimeFormat)

        println("\nIn tilesRoute...\n")
        println(s"layer: ${layer}")
        println(s"zoom: ${zoom}")
        println(s"X: $x, Y: $y")
        println(s"Time: $time")

        complete {
          Future {
            val tileOpt =
              readerSet.readSinglebandTile(layer, zoom, x, y, time)
              //readerSet.readMultibandTile(layer, zoom, x, y, time)

            tileOpt.map { tile =>
              //println(s"bands: ${tile.bandCount}")
              //println(s"tile.band(0).findMinMaxDouble: ${tile.band(0).findMinMaxDouble}")

              val png =
                operationOpt match {
                  case Some(op) =>
                    op match {
                      case "ndvi" =>
                        //Render.image(tile) // Render.ndvi(tile)
                        val ndvi = tile.convert(DoubleConstantNoDataCellType)
                        ndvi.renderPng(ndviColorMap)
                      case "ndwi" =>
                        //Render.image(tile) // Render.ndwi(tile)
                        val ndvi = tile.convert(DoubleConstantNoDataCellType)
                        ndvi.renderPng(ndviColorMap)
                      case _ =>
                        sys.error(s"UNKNOWN OPERATION $op")
                    }
                  case None =>
                    //Render.image(tile)
                    val ndvi = tile.convert(DoubleConstantNoDataCellType)
                    ndvi.renderPng(ndviColorMap)
                }
              //println(s"BYTES: ${png.bytes.length}")
              pngAsHttpResponse(png)
            }
          }
        }
      }
    }

  def polygonalMeanRoute = {
    println("\nIn polygonalMeanRoute...\n")
    import spray.json.DefaultJsonProtocol._

    pathPrefix(Segment / Segment) { (layer, op) =>
      parameters('time, 'otherTime ?, 'zoom ?) { (time, otherTime, zoomLevel) =>
        cors() {
          post {
            entity(as[String]) { json =>
              complete {
                Future {
                  val zoom = zoomLevel
                    .map(_.toInt)
                    .getOrElse(metadataReader.layerNamesToMaxZooms(layer))

                  val catalog = readerSet.layerReader
                  val layerId = LayerId(layer, zoom)

                  val rawGeometry = try {
                    json.parseJson.convertTo[Geometry]
                  } catch {
                    case e: Exception => sys.error("THAT PROBABLY WASN'T GEOMETRY")
                  }
                  val geometry = rawGeometry match {
                    case p: Polygon => MultiPolygon(p.reproject(LatLng, WebMercator))
                    case mp: MultiPolygon => mp.reproject(LatLng, WebMercator)
                    case _ => sys.error(s"BAD GEOMETRY")
                  }
                  val extent = geometry.envelope

                  val fn = op match {
                    case "ndvi" => NDVI.apply(_)
                    case "ndwi" => NDWI.apply(_)
                    case _ => sys.error(s"UNKNOWN OPERATION")
                  }

                  println(s"\nLayer: ${layer}")
                  println(s"Op: ${op}")

                  println(s"DateTimeFormat: ${dateTimeFormat}")
                  println(s"Time: ${time}")
                  println(s"OtherTime: ${otherTime}")
                  println(s"ZoomLevel: ${zoomLevel}")

                  println(s"Extent: ${extent}")
                  println(s"WKT: ${rawGeometry}\n")

                  val dateFrom = "2016-12-09T12:12:12-0400"
                  val dateTo = "2016-12-31T12:12:12-0400"

                  val rdd = catalog
                    .query[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](layerId)
                    .where(Intersects(extent))
                    .where(Between(ZonedDateTime.parse(dateFrom, dateTimeFormat), ZonedDateTime.parse(dateTo, dateTimeFormat)))
                    .result

                  val timeSeriesAnswer = ContextRDD(rdd.map({ v => (v) }), rdd.metadata).polygonalMean(geometry)
                  val imgNum = ContextRDD(rdd.mapValues({ v => (v) }), rdd.metadata).count()


                  val mt = rdd.metadata.mapTransform

                  val answer4 = rdd
                    .map { case (k, tile) =>
                      val re = RasterExtent(mt(k), tile.cols, tile.rows)
                      val mn = tile.polygonalMean(extent, geometry)
                      println(s"mean ndvi for key $k equals $mn")
                      (k, tile)
                    }.collect()

                  //val timeSeriesAnswer2 = ContextRDD(rdd.mapValues({ v => (v) }), rdd.metadata).polygonalMean(geometry)
                  //val timeSeriesAnswe = rdd.collect().foreach(println)

                  //println(s"\ntimeSeriesAnswer: ${timeSeriesAnswe}\n")

                  println(s"\ntimeSeriesAnswer: ${timeSeriesAnswer} (for $imgNum images)\n")

                  val rdd1 = catalog
                    .query[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](layerId)
                    .where(At(ZonedDateTime.parse(time, dateTimeFormat)))
                    .where(Intersects(extent))
                    .result
                  val answer1 = ContextRDD(rdd1.mapValues({ v => (v) }), rdd1.metadata).polygonalMean(geometry)

                  println(s"\nAnswer1: ${answer1}")

                  val answer2: Double = otherTime match {
                    case None => 0.0
                    case Some(otherTime) =>
                      val rdd2 = catalog
                        .query[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](layerId)
                        .where(At(ZonedDateTime.parse(otherTime, dateTimeFormat)))
                        .where(Intersects(extent))
                        .result

                      ContextRDD(rdd2.mapValues({ v => fn(v) }), rdd2.metadata).polygonalMean(geometry)
                  }

                  println(s"\nAnswer2: ${answer2}")

                  val answer = answer1 - answer2

                  println(s"\nAnswer: ${answer}")

                  JsObject("answer" -> JsNumber(answer))
                }
              }
            }
          }
        }
      }
    }
  }

  def timeseriesRoute = {
    println("\nIn timeseriesRoute...\n")
    import spray.json.DefaultJsonProtocol._

    pathPrefix(Segment / Segment) { (layer, op) =>
      parameters('lat, 'lng, 'zoom ?) { (lat, lng, zoomLevel) =>
        cors() {
          complete {
            Future {
              val zoom = zoomLevel
                .map(_.toInt)
                .getOrElse(metadataReader.layerNamesToMaxZooms(layer))

              val catalog = readerSet.layerReader
              val layerId = LayerId(layer, zoom)
              val point = Point(lng.toDouble, lat.toDouble).reproject(LatLng, WebMercator)

              println(s"\nLayer: $layer")
              println(s"Op: $op")
              println(s"Lat: $lat")
              println(s"Lng: $lng")
              println(s"ZoomLevel: $zoomLevel")
              println(s"LayerId: $layerId")
              println(s"Point: $point")

              // Wasteful but safe
              val fn = op match {
                case "ndvi" => NDVI.apply(_)
                case "ndwi" => NDWI.apply(_)
                case _ => sys.error(s"UNKNOWN OPERATION")
              }

              val rdd = catalog.query[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](layerId)
                .where(Intersects(point.envelope))
                .result

              val mt = rdd.metadata.mapTransform

              println(s"RDD md: $mt")

              val answer = rdd
                .map { case (k, tile) =>
                  // reconstruct tile raster extent so we can map the point to the tile cell
                  val re = RasterExtent(mt(k), tile.cols, tile.rows)
                  val (tileCol, tileRow) = re.mapToGrid(point)
                  val ret = tile.getDouble(tileCol, tileRow)
                  println(s"\n$point equals $ret at ($tileCol, $tileRow) in tile $re ")
                  (k.time, ret)
                }
                .collect
                .filterNot(_._2.isNaN)
                .toJson

              println(s"\nAnswer: ${answer}\n")

              JsObject("answer" -> answer)
            }
          }
        }
      }
    }
  }

  def readallRoute = {
    println("\nreadallRoute")
    import spray.json._
    import spray.json.DefaultJsonProtocol._

    pathPrefix(Segment / IntNumber) { (layer, zoom) =>
      get {
        cors() {
          complete {
            Future {
              println("\nIn readallRoute... (GET)\n")

              val catalog = readerSet.layerReader
              val ccatalog = readerSet.layerCReader
              val id = LayerId(layer, zoom)

              JsObject("result" -> ((1 to 20) map { case i =>
                val (objrdd, strrdd) = timedCreate(
                  catalog
                    .query[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](id)
                    .result.count()
                )

                val (objc, strc) = timedCreate(
                  ccatalog
                    .query[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](id)
                    .result.length
                )

                JsObject(
                  "n" -> i.toString.toJson,
                  "obj_rdd" -> objrdd.toJson,
                  "time_rdd" -> strrdd.toJson,
                  "obj_collection" -> objc.toJson,
                  "time_collection" -> strc.toJson,
                  "conf" -> ConfigFactory.load().getObject("geotrellis").render(ConfigRenderOptions.concise()).toJson
                )
              }).toList.toJson)
            }
          }
        }
      }
    }
  }

  def diffRoute =
    pathPrefix(Segment / IntNumber / IntNumber / IntNumber) { (layer, zoom, x, y) =>
      parameters('time1, 'time2, 'breaks ?, 'operation ?) { (timeString1, timeString2, breaksStrOpt, operationOpt) =>
        val time1 = ZonedDateTime.parse(timeString1, dateTimeFormat)
        val time2 = ZonedDateTime.parse(timeString2, dateTimeFormat)
        complete {
          Future {
            println("\ndiffRoute")
            val tileOpt1 =
              readerSet.readMultibandTile(layer, zoom, x, y, time1)

            val tileOpt2 =
              tileOpt1.flatMap { tile1 =>
                readerSet.readMultibandTile(layer, zoom, x, y, time2).map { tile2 => (tile1, tile2) }
              }

            tileOpt2.map { case (tile1, tile2) =>
              val png =
                operationOpt match {
                  case Some(op) =>
                    op match {
                      case "ndvi" =>
                        Render.ndvi(tile1, tile2)
                      case "ndwi" =>
                        Render.ndwi(tile1, tile2)
                      case _ =>
                        sys.error(s"UNKNOWN OPERATION $op")
                    }
                  case None =>
                    ???
                }

              pngAsHttpResponse(png)
            }
          }
        }
      }
    }
}
