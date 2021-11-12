package de.hpi.data_preparation.socrata.history

import de.hpi.data_preparation.socrata.DatasetInstance
import de.hpi.data_preparation.socrata.io.Socrata_IOService

import java.io.File

object SimplfiedDataVersionHistoryRepair extends App {
  Socrata_IOService.socrataDir = args(0)
  val list = DatasetVersionHistory.fromJsonObjectPerLineFile(Socrata_IOService.getCleanedVersionHistoryFile().getAbsolutePath)
  list.foreach(h => {
    val versions = h.versionsWithChanges
    versions.foreach(v => {
      val curFile = Socrata_IOService.getSimplifiedDatasetFile(DatasetInstance(h.id, v))
      val simplfiedFileExists = new File(curFile).exists()
      if(!simplfiedFileExists){
        println(h.id + " " + v.format(Socrata_IOService.dateTimeFormatter))
      }

    })
  })

}
