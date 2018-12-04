package observatory

import org.apache.log4j.Logger

import scala.collection.parallel.immutable.ParMap


/**
  * 4th milestone: value-added information
  */
object Manipulation {
  val logger = Logger.getLogger(this.getClass)

  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {
    //logger.debug("Start grid for " + temperatures.head)
    val gridLocations: Seq[GridLocation] =
      for{
        lat <- -89 to 90
        lon <- -180 to 179
      } yield GridLocation(lat, lon)

    val grid: ParMap[GridLocation, Temperature] =
      gridLocations.par.map(gl => (gl, Visualization.predictTemperature(temperatures, Location(gl.lat, gl.lon)))).toMap
    //logger.debug("End grid for " + temperatures.head)
    (gLocation) => grid(gLocation)
  }

  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
    val gridFunctions: Iterable[GridLocation => Temperature] = temperaturess.map(makeGrid)
    (gLocation) => {
      val annualTemperatures: Iterable[Temperature] = gridFunctions.map(grid => grid(gLocation))
      annualTemperatures.sum / annualTemperatures.size
    }
  }

  def averageGrids(grids: Iterable[GridLocation => Temperature]): GridLocation => Temperature = {
    //val gridFunctions: Iterable[GridLocation => Temperature] = temperaturess.map(makeGrid)
    (gLocation) => {
      val annualTemperatures: Iterable[Temperature] = grids.map(grid => grid(gLocation))
      annualTemperatures.sum / annualTemperatures.size
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param normals A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Temperature)], normals: GridLocation => Temperature): GridLocation => Temperature = {
    val grid: GridLocation => Temperature = makeGrid(temperatures)
    (gLocation) => grid(gLocation) - normals(gLocation)
  }

}

