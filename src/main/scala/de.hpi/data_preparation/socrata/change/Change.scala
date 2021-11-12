package de.hpi.data_preparation.socrata.change

import de.hpi.data_preparation.socrata.{JsonReadable, JsonWritable}

import java.time.LocalDate

case class Change(t:LocalDate, e:Long, pID:Int, value:Any) extends JsonWritable[Change]{
}

object Change extends JsonReadable[Change]
