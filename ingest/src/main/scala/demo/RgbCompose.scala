package demo

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._
import com.typesafe.config.ConfigFactory

/**
  * Created by kkaralas on 4/11/17.
  */
object RgbCompose {
  val output = "data/S2A_USER_MSI_L2A_TL_SGS__20160404T132741_A004094_T34SFJ_10m.tif"

  //constants to differentiate which bands to use
  val R_BAND = "B04"
  val G_BAND = "B03"
  val B_BAND = "B02"

  // Path to sentinel band geotiffs
  def bandPath(b: String) = s"/home/kkaralas/Documents/shared/rgb/ini/S2A_USER_PRD_MSIL2A_PDMC_20160406T143016_R093_V20160404T092409_20160404T092409.SAFE/GRANULE/S2A_USER_MSI_L2A_TL_SGS__20160404T132741_A004094_T34SFJ_N02.01/IMG_DATA/R10m/S2A_USER_MSI_L2A_TL_SGS__20160404T132741_A004094_T34SFJ_${b}_10m.tif"

  def main(args: Array[String]): Unit = {
    // Read in the red band
    println("Reading in the red band...")
    val rGeoTiff = SinglebandGeoTiff(bandPath(R_BAND))

    // Read in the green band
    println("Reading in the green band...")
    val gGeoTiff = SinglebandGeoTiff(bandPath(G_BAND))

    // Read in the blue band
    println("Reading in the blue band...")
    val bGeoTiff = SinglebandGeoTiff(bandPath(B_BAND))

    // GeoTiffs have more information we need; just grab the Tile out of them.
    val (rTile, gTile, bTile) = (rGeoTiff.tile, gGeoTiff.tile, bGeoTiff.tile)

    // Create a multiband tile with our two masked red and infrared bands.
    val mb = ArrayMultibandTile(rTile, gTile, bTile).convert(IntConstantNoDataCellType)

    // Create a multiband geotiff from our tile, using the same extent and CRS as the original geotiffs.
    println("Writing out the multiband R + G + B tile...")
    MultibandGeoTiff(mb, rGeoTiff.extent, rGeoTiff.crs).write(output)
  }
}
