package de.hpi.data_preparation.socrata.column_history_export

import java.io.{File, PrintWriter}
import java.time.LocalDate

case class ColumnVersion(revisionID: String,
                         revisionDate: String,
                         values: Set[String],
                         columnNotPresent:Boolean){

  if(columnNotPresent)
    assert(values.isEmpty)

  def timestamp = LocalDate.parse(revisionDate)

  def isDelete = columnNotPresent

}

object ColumnVersion {


  def COLUMN_DELETE(revisionID: String, revisionDate: String): ColumnVersion = ColumnVersion(revisionID,revisionDate,Set(),true)
}
