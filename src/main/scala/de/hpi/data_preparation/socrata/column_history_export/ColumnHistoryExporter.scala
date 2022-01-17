package de.hpi.data_preparation.socrata.column_history_export

import de.hpi.data_preparation.socrata.simplified.RelationalDataset

import java.io.{File, PrintWriter}
import java.time.{Instant, LocalDate, ZoneOffset}

class ColumnHistoryExporter(id: String, versions: IndexedSeq[LocalDate],resultDir:File) {

  val columns = collection.mutable.HashMap[Int,ColumnHistory]()

  def addDeleteIfNonEmpty(colID: Int, dateAsInstant: Instant) = {
    val ch = columns(colID)
    if(!ch.columnVersions.last.isDelete)
      ch.columnVersions.append(ColumnVersion.COLUMN_DELETE(getRevisionID(colID,dateAsInstant),dateAsInstant.toString))
  }

  def exportAll() = {
    versions.sorted.foreach(v => {
      val ds = RelationalDataset.load(id,v)
      val dateAsInstant = v.atStartOfDay().toInstant(ZoneOffset.UTC)
      if(ds.isEmpty){
        columns.keySet.foreach(colID => addDeleteIfNonEmpty(colID,dateAsInstant))
      } else {
        val presentIDs = ds.getColIDs.toSet.toIndexedSeq.sorted
        presentIDs.foreach(colID => {
          val values = ds.getColValuesForColID(colID)
          val dsAndColID = getDSAndColID(colID)
          if(!columns.contains(colID)){
            //just add new!
            val ch = ColumnHistory(dsAndColID,id,id,id,collection.mutable.ArrayBuffer[ColumnVersion]())
            ch.columnVersions.append(ColumnVersion(getRevisionID(colID, dateAsInstant),dateAsInstant.toString,values,values.isEmpty))
            columns.put(colID,ch)
          } else{
            val ch = columns(colID)
            //check
            val oldValues = ch.columnVersions.last.values
            if(values==oldValues){
              //we just skip this
            } else {
              ch.columnVersions.append(ColumnVersion(getRevisionID(colID, dateAsInstant),dateAsInstant.toString,values,values.isEmpty))
            }
          }
        })
        columns.keySet.diff(presentIDs.toSet).foreach(colID => addDeleteIfNonEmpty(colID,dateAsInstant))
      }
    })
    val outFile = new File(resultDir.getAbsolutePath + s"/${id}.json")
    val pr = new PrintWriter(outFile)
    columns.values.foreach(ch => {
      ch.appendToWriter(pr,false,true)
    })
    pr.close()
  }

  private def getDSAndColID(colID: Int) = {
    id + "|" + colID
  }

  private def getRevisionID(colID: Int, dateAsInstant: Instant) = {
    getDSAndColID(colID) + "|" + dateAsInstant.toString
  }
}
