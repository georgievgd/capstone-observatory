package observatory

import observatory.LayerName.{Deviations, Temperatures}

/**
  * 6th (and last) milestone: user interface polishing
  */
object Interaction2 {

  /**
    * @return The available layers of the application
    */
  def availableLayers: Seq[Layer] = {
    Seq(
    Layer(Temperatures, temperatureColors, 1975 to 2015),
    Layer(Deviations, deviationColors, 1975 to 2015)
    )
  }

  val temperatureColors: Seq[(Temperature,Color)] = Seq(
    (-60, Color(0, 0, 0)),
    (-50, Color(33, 0, 107)),
    (-27, Color(255, 0, 255)),
    (-15, Color(0, 0, 255)),
    (0, Color(0, 255, 255)),
    (12, Color(255, 255, 0)),
    (32, Color(255, 0, 0)),
    (60, Color(255, 255, 255))
  )

  val deviationColors: Seq[(Temperature,Color)] = Seq(
    (-7, Color(0, 0, 255)),
    (-2, Color(0, 255, 255)),
    (0, Color(255, 255, 255)),
    (2, Color(255, 255, 0)),
    (4, Color(255, 0, 0)),
    (7, Color(0, 0, 0))
  )

  /**
    * @param selectedLayer A signal carrying the layer selected by the user
    * @return A signal containing the year bounds corresponding to the selected layer
    */
  def yearBounds(selectedLayer: Signal[Layer]): Signal[Range] = {
    Signal(selectedLayer().bounds)
  }

  /**
    * @param selectedLayer The selected layer
    * @param sliderValue The value of the year slider
    * @return The value of the selected year, so that it never goes out of the layer bounds.
    *         If the value of `sliderValue` is out of the `selectedLayer` bounds,
    *         this method should return the closest value that is included
    *         in the `selectedLayer` bounds.
    */
  def yearSelection(selectedLayer: Signal[Layer], sliderValue: Signal[Year]): Signal[Year] = {
    val bounds = selectedLayer().bounds
    if(selectedLayer().bounds.contains(sliderValue())){
      Signal(sliderValue())
    }else if(selectedLayer().bounds.min >= sliderValue()){
      Signal(selectedLayer().bounds.min)
    }else {
      Signal(selectedLayer().bounds.max)
    }
  }

  /**
    * @param selectedLayer The selected layer
    * @param selectedYear The selected year
    * @return The URL pattern to retrieve tiles
    */
  def layerUrlPattern(selectedLayer: Signal[Layer], selectedYear: Signal[Year]): Signal[String] = {
    Signal {
      val layer = selectedLayer()
      val name = layer.layerName
      val year = selectedYear()
      name match {
        case Temperatures => s"target/${Temperatures.id}/$year/{z}/{x}-{y}.png"
        case Deviations => s"target/${Deviations.id}/$year/{z}/{x}-{y}.png"
      }
    }
  }

  /**
    * @param selectedLayer The selected layer
    * @param selectedYear The selected year
    * @return The caption to show
    */
  def caption(selectedLayer: Signal[Layer], selectedYear: Signal[Year]): Signal[String] = {
    Signal {
      val layer = selectedLayer()
      val name = layer.layerName
      val year = selectedYear()
      name match {
        case Temperatures => s"Temperatures ($year)"
        case Deviations => s"Deviations ($year)"
      }
    }
  }

}

sealed abstract class LayerName(val id: String)
object LayerName {
  case object Temperatures extends LayerName("temperatures")
  case object Deviations extends LayerName("deviations")
}

/**
  * @param layerName Name of the layer
  * @param colorScale Color scale used by the layer
  * @param bounds Minimum and maximum year supported by the layer
  */
case class Layer(layerName: LayerName, colorScale: Seq[(Temperature, Color)], bounds: Range)

