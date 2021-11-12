package de.hpi.data_preparation.socrata.history

import de.hpi.data_preparation.socrata.io.Socrata_IOService

object VersionHistoryConstructionMain extends App {
  Socrata_IOService.socrataDir = args(0)
  val versionHistoryConstruction = new VersionHistoryConstruction()
  versionHistoryConstruction.constructVersionHistory()
}
