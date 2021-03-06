package de.hpi.data_preparation.socrata.history

import de.hpi.data_preparation.socrata.io.Socrata_IOService

import java.time.LocalDate

object VersionHistoryConstructionMain extends App {
  Socrata_IOService.socrataDir = args(0)
  val maxDate = LocalDate.parse(args(1))
  val versionHistoryConstruction = new VersionHistoryConstruction()
  versionHistoryConstruction.constructVersionHistory(maxDate)
}
