package demo

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._
import com.typesafe.config.ConfigFactory

/**
  * Created by kkaralas on 4/11/17.
  */
object RgbCompose {
  val output = "data/rgb.tif"

  //constants to differentiate which bands to use
  val R_BAND = "00"
  val G_BAND = "01"
  val B_BAND = "02"

  // Path to sentinel band geotiffs
  def bandPath(b: String) = s"/home/kkaralas/Documents/shared/data/geotiffs/S2A_USER_MSI_L2A_TL_MPS__20160802T132315_A005810_T34TEL_B${b}_10m.tif"

  def main(args: Array[String]): Unit = {
    // Read in the red band
    println("Reading in the red band...")
    val rGeoTiff = SinglebandGeoTiff(bandPath("B4"))

    // Read in the green band
    println("Reading in green band...")
    val gGeoTiff = SinglebandGeoTiff(bandPath("B3"))

    // Read in the blue band
    println("Reading in the blue band...")
    val bGeoTiff = SinglebandGeoTiff(bandPath("B2"))

    // GeoTiffs have more information we need; just grab the Tile out of them.
    val (rTile, gTile, bTile) = (rGeoTiff.tile, gGeoTiff.tile, bGeoTiff.tile)

    // Create a multiband tile with our two masked red and infrared bands.
    val mb = ArrayMultibandTile(rTile, gTile, bTile).convert(IntConstantNoDataCellType)

    // Create a multiband geotiff from our tile, using the same extent and CRS as the original geotiffs.
    println("Writing out the multiband R + G + B tile...")
    MultibandGeoTiff(mb, rGeoTiff.extent, rGeoTiff.crs).write(output)
  }
}