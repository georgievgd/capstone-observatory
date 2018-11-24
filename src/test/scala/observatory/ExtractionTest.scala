package observatory

import com.sksamuel.scrimage.Pixel
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

class ExtractionTest extends FunSuite {

  test("locateTemperatures should work as specified") {
    val loctemps = Extraction.locateTemperatures(2015, "/test-stations.csv", "/test-2015.csv")
    assert(loctemps.size === 6, "Result size is incorrect")
  }

  test("averageYearlyTemps should work as specified") {
    val loctemps = Extraction.locateTemperatures(2015, "/test-stations.csv", "/test-2015.csv")
    //println(loctemps.mkString("\n"))
    val tempAve = Extraction.locationYearlyAverageRecords(loctemps)
    assert(tempAve.size === 3, "Result size is incorrect")
    assert(tempAve.exists(pair => math.round(pair._2) == 28),  "Ave temp incorrect")
  }

  test("sphericalDistance should work correctly") {
    val loc1 = Location(30, 60)
    val loc2 = Location(-30, -120)
    val loc3 = Location(30, 90)
    val loc4 = Location(45.0,-90.0)
    val loc5 = Location(42.0, -80.0)
    val loc6 = Location(-45.0,0.0)
//    println(s"dist loc4 and loc5 : ${Utils.sphericalDistance(loc4, loc5)}, central angle: ${Utils.centralAngle1(loc4,loc5)}")
//    println(s"dist loc5 and loc6 : ${Utils.sphericalDistance(loc5, loc6)}, central angle: ${Utils.centralAngle1(loc5,loc6)}")
    assert(Utils.sphericalDistance(loc1, loc2) === Utils.EARTH_RADIUS*Utils.PI, "Antipods are incorrect")
    assert(Utils.sphericalDistance(loc1, loc1) === 0.0, "Same points result is incorrect")
    assert(Utils.sphericalDistance(loc1, loc3) === 2880.5117204961302, "2 points distance is incorrect")
    assert(Utils.sphericalDistance(loc1, loc3) < Utils.sphericalDistance(loc1, loc2), "loc1 should be closer to loc3 than to loc2")
    assert(Utils.sphericalDistance(loc4, loc5) < Utils.sphericalDistance(loc5, loc6), "loc5 should be closer to loc4 than to loc6")
  }

//  test("central angle should work correctly") {
//    val loc1 = Location(0, 0)
//    val loc2 = Location(0, -180)
//    val loc3 = Location(0, 90)
//    val loc4 = Location(0, 10)
//    val loc5 = Location(0, 1)
//    val loc6 = Location(-45.0,0.0)
//    println(s"Central angle 1n2: ${Utils.centralAngle1(loc1,loc2)}")
//    println(s"Central angle 1n3: ${Utils.centralAngle1(loc1,loc3)}")
//    println(s"Central angle 1n4: ${Utils.centralAngle1(loc1,loc4)}")
//    println(s"Central angle 1n5: ${Utils.centralAngle1(loc1,loc5)}")
//  }

  test("interpolateColor should work correctly") {
    assert(Visualization.interpolateColor(Utils.colorScale, 0) === Color(0, 255, 255), "color for 0 is incorrect")
    assert(Visualization.interpolateColor(Utils.colorScale, -10) === Color(0,85,255), "color for -10 is incorrect")
    assert(Visualization.interpolateColor(Utils.colorScale, 33) === Color(255,9,9), "color for 33 is incorrect")
    assert(Visualization.interpolateColor(Utils.colorScale, 63) === Color(255, 255, 255), "color for 63 is incorrect")
    assert(Visualization.interpolateColor(Utils.colorScale, -55) === Color(17,0,54), "color for -55 is incorrect")
    assert(Visualization.interpolateColor(Utils.colorScale, -100) === Color(0, 0, 0), "color for -100 is incorrect")

  }

  test("predicted temperature proximity 1") {
    val t1 = 0.0
    val t2 = -30.12345
    val c1 = Color(255, 0, 0)
    val c2 = Color(0, 0, 255)
    val colorScale: Seq[(Temperature,Color)] = Seq(
      (t1, c1),
      (t2, c2)
    )
    val temps = Seq(
      (Location(45.0, -90.0), t1),
      (Location(-45.0, 0.0), t2)
    )
    val checkLocation1 = Location(42, -80)
    val predictedTemp1 = Visualization.predictTemperature(temps, checkLocation1)
    val checkColor1 = Visualization.interpolateColor(colorScale, predictedTemp1)
    assert(predictedTemp1 > -15, "predicted temp should be closer to t1")
    assert(checkColor1.red > 127, "red color should be closer to c1")
    assert(checkColor1.blue < 127, "blue color should be closer to c1")
  }

  test("predicted temperature proximity 2") {
    val t1 = 0.0
    val t2 = -90.28442553640245
    val c1 = Color(255, 0, 0)
    val c2 = Color(0, 0, 255)
    val colorScale: Seq[(Temperature,Color)] = Seq(
      (t1, c1),
      (t2, c2)
    )
    val temps = Seq(
      (Location(45.0,-90.0),t1),
      (Location(-45.0,0.0),t2)
    )
    val checkLocation1 = Location(90.0,-180.0)
    val predictedTemp1 = Visualization.predictTemperature(temps, checkLocation1)
    val checkColor1 = Visualization.interpolateColor(colorScale, predictedTemp1)
    println(s"temp $predictedTemp1, color ${checkColor1}")
    assert(predictedTemp1 > -10, "predicted temp should be closer to t1")
    assert(checkColor1.red > 127, "red color should be closer to c1")
    assert(checkColor1.blue < 127, "blue color should be closer to c1")
  }

  test("visualisation") {
    val t1 = -69.16307455398416
    val t2 = 0.0
    val c1 = Color(255, 0, 0)
    val c2 = Color(0, 0, 255)
    val colorScale: Seq[(Temperature,Color)] = Seq(
      (t1, c1),
      (t2, c2)
    )
    val temps = Seq(
      (Location(45.0,-90.0),t1),
      (Location(-45.0,0.0),t2)
    )
    val image = Visualization.visualize(temps, colorScale)
    val topLeftPixel = image.pixel(0,0)
    //println(s"top-left pixel red ${topLeftPixel.red}, blue ${topLeftPixel.blue}")
    assert(topLeftPixel.red > 127, "red color should be closer to c1")
    assert(topLeftPixel.blue < 127, "blue color should be closer to c1")
  }

  test("tile2location") {
    val tile = Tile(65544,43582,17)
    val location = Interaction.tile2location(tile)
    assert(math.round(location.lat*100000) === 5151216, "incorrect lat")
    assert(math.round(location.lon*100000) === 2197, "incorrect lon")
  }

  test("location2tile") {
    val loc = Location(51.51202,0.02435)
    val tile = Interaction.location2tile(loc, 17)
    assert(tile.x === 65544, "incorrect tile x")
    assert(tile.y === 43582, "incorrect tile y")
  }
  
}