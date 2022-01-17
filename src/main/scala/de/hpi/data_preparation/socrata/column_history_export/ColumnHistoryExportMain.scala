package de.hpi.data_preparation.socrata.column_history_export

import de.hpi.data_preparation.socrata.history.DatasetVersionHistory
import de.hpi.data_preparation.socrata.io.{GLOBAL_CONFIG, Socrata_IOService}

import java.io.File

object ColumnHistoryExportMain extends App {
  Socrata_IOService.socrataDir = args(0)
  val id = args(1)
  val resultDIr = new File(args(2))
  var histories = DatasetVersionHistory.fromJsonObjectPerLineFile(Socrata_IOService.getCleanedVersionHistoryFile().getAbsolutePath)
    .map(h => (h.id, h))
    .toMap
  val versions = histories(id).allVersionsIncludingDeletes
  val exporter = new ColumnHistoryExporter(id,versions,resultDIr)
  exporter.exportAll()


}
