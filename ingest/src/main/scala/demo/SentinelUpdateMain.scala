package demo

import java.io.File
import java.net.URI

import geotrellis.proj4._
import geotrellis.proj4.WebMercator
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.vector.io._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.cassandra._
import geotrellis.spark.io.file.{FileAttributeStore, FileLayerUpdater, FileLayerWriter}
import geotrellis.spark.pyramid._
import geotrellis.spark.tiling._
import geotrellis.vector.Extent
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import spray.json._
import spray.json.DefaultJsonProtocol._


import org.apache.hadoop.fs._
import org.apache.spark.deploy.SparkHadoopUtil
import java.net.URI






/**
  * Created by kkaralas on 3/31/17.
  */
object SentinelUpdateMain extends App {

  // Update existing layer with new images
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }



  def fullPath(path: String) = new java.io.File(path).getAbsolutePath

  val instance: CassandraInstance = new CassandraInstance {
    override val username = "cassandra"
    override val password = "cassandra"
    override val hosts = Seq("172.16.3.123", "172.16.3.135")
    override val localDc = "testdc"
    override val replicationStrategy = "SimpleStrategy"
    override val allowRemoteDCsForLocalConsistencyLevel = false
    override val usedHostsPerRemoteDc = 0
    override val replicationFactor = 1
  }

  val keyspace: String = "geotrellis"
  val layerName = "catalog"
  val dataTable: String = layerName

  // Setup Spark to use Kryo serializer
  val conf =
    new SparkConf()
      .setMaster("spark://172.16.3.123:7077")
      .setAppName("Spark Update")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

  println("\nSentinelUpdateMain\n")

  //val files = getListOfFiles("/home/kkaralas/Documents/shared/data/test2")
  //val fs = FileSystem.get(new Configuration());
  //val files = fs.listStatus(new Path("hdfs://72.16.3.123:9000/ndvi_rem"));

  //val configuration = new Configuration()
  //val fs = FileSystem.get(new URI("hdfs://72.16.3.123:9000/ndvi_rem/S2A_USER_MTD_SAFL2A_PDMC_20160514T180249_R093_V20160514T092034_20160514T092928_NDVI.tif"), configuration)
  //val files = fs.listStatus(new Path("hdfs://72.16.3.123:9000/ndvi_rem/S2A_USER_MTD_SAFL2A_PDMC_20160514T180249_R093_V20160514T092034_20160514T092928_NDVI.tif"))


  implicit val sc = new SparkContext(conf)

  //val files = FileSystem.get(sc.hadoopConfiguration).listStatus(new Path("hdfs://72.16.3.123:9000/ndvi_rem"))

  // Use the same Cassandra instance used for the first ingest
  val attributeStore = CassandraAttributeStore(instance)
  val updater = CassandraLayerUpdater(attributeStore)

  // We'll be tiling the images using a zoomed layout scheme in the web mercator format
  val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

  //files.foreach { file =>
    //println(s"\nUpdating layer with image: ${file.getPath.toString()} ...\n")

    //val source = sc.hadoopTemporalGeoTiffRDD(file.getPath.toString())
    val source = sc.hadoopTemporalGeoTiffRDD("hdfs://172.16.3.123:9000/ndvi_rem/S2A_USER_MTD_SAFL2A_PDMC_20160514T180249_R093_V20160514T092034_20160514T092928_NDVI.tif")
    val (_, md) = TileLayerMetadata.fromRdd[TemporalProjectedExtent, Tile, SpaceTimeKey](source, FloatingLayoutScheme(256))

    // Keep the same number of partitions after tiling
    val tilerOptions = Tiler.Options(resampleMethod = NearestNeighbor)
    val tiled = ContextRDD(source.tileToLayout[SpaceTimeKey](md, tilerOptions), md)
    val (zoom, reprojected) = tiled.reproject(WebMercator, ZoomedLayoutScheme(WebMercator), NearestNeighbor)

    // Pyramiding up the zoom levels, update our tiles out to Cassandra
    Pyramid.upLevels(reprojected, layoutScheme, zoom, 0, NearestNeighbor) { (rdd, z) =>
      val layerId = LayerId(layerName, z)

      val keySpace = attributeStore.readKeyIndex[SpaceTimeKey](layerId).keyBounds
      val kb = rdd.metadata.bounds match {
        case kb: KeyBounds[SpaceTimeKey] => kb
      }

      println(s"\nPrinting bounds in Update...")
      println(s"AttributeStore keySpace ($layerId): ${keySpace}")
      println(s"RDD kb ($layerId): ${kb}")
      println(s"keySpace contains kb: ${keySpace contains kb}\n")

      updater.update[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](layerId, rdd)

      if (z == 0) {
        val id = LayerId(layerName, 0)

        val times = attributeStore.read[Array[Long]](id, "times") // read times
        attributeStore.delete(id, "times") // delete it
        attributeStore.write(id, "times", // write new on the zero zoom level
          (times ++ rdd
            .map(_._1.instant)
            .countByValue
            .keys.toArray
            .sorted))

        val (extent, crs) = attributeStore.read[(Extent, CRS)](id, "extent")
        attributeStore.delete(id, "extent")
        attributeStore.write(id, "extent", extent.combine(md.extent) -> crs)
      }
    }

  //}

  sc.stop()
}


