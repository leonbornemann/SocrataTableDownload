package de.hpi.data_preparation.socrata.column_history_export

import de.hpi.data_preparation.socrata.{JsonReadable, JsonWritable}

import java.io.File
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.collection.mutable.ArrayBuffer

case class ColumnHistory(id: String,
                         tableId: String,
                         pageID: String,
                         pageTitle: String,
                         columnVersions: ArrayBuffer[ColumnVersion]
                        ) extends JsonWritable[ColumnHistory]{

  def columnVersionsSorted: Boolean = {
    val withIndex = columnVersions
      .zipWithIndex
    withIndex
      .forall(t => t._2==0 || columnVersions(t._2-1).timestamp.isBefore(t._1.timestamp))
  }

}
object ColumnHistory extends JsonReadable[ColumnHistory] {


}
