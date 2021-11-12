package de.hpi.data_preparation.socrata.diff.syntactic

import de.hpi.data_preparation.socrata.io.Socrata_IOService

object DiffCreationMain extends App {
  Socrata_IOService.socrataDir = args(0)
  Socrata_IOService.printSummary()
  val transformer = new DiffManager(7)
  transformer.calculateAllDiffs()
}
