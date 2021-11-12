package de.hpi.data_preparation.socrata.simplified

import de.hpi.data_preparation.socrata.io.Socrata_IOService

import scala.io.Source

object ColumnOrderRestoreByIdListMain extends App {
  Socrata_IOService.socrataDir = args(0)
  val ids = Source.fromFile(args(1))
    .getLines()
    .toSet
  val restorer = new ColumnOrderRestorer
  restorer.restoreForAllForIdList(ids)
}
