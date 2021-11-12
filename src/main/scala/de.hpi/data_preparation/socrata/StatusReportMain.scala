package de.hpi.data_preparation.socrata

import de.hpi.data_preparation.socrata.io.Socrata_IOService

object StatusReportMain extends App {
  Socrata_IOService.socrataDir = args(0)
  Socrata_IOService.printSummary()
}
