package de.hpi.data_preparation.socrata.io

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object GLOBAL_CONFIG {

  var STANDARD_TIME_FRAME_START = LocalDate.parse("2019-11-01")
  var STANDARD_TIME_FRAME_END = LocalDate.parse("2020-04-30")
  val dateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE
}
