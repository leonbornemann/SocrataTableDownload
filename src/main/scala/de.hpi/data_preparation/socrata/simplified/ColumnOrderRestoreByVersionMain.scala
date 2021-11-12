package de.hpi.data_preparation.socrata.simplified

import com.typesafe.scalalogging.StrictLogging
import de.hpi.data_preparation.socrata.io.Socrata_IOService

import java.time.LocalDate

object ColumnOrderRestoreByVersionMain extends App with StrictLogging{

  Socrata_IOService.socrataDir = args(0)
  println(args.toSeq)
  val version = LocalDate.parse(args(1),Socrata_IOService.dateTimeFormatter)
  val id = if(args.length>2) Some(args(2)) else None
  val restorer = new ColumnOrderRestorer()
  if(id.isDefined){
    logger.debug(s"Redoing Column Ordering for single dataset version: $id and $version")
    restorer.restoreInDataset(id.get,version,false)
  } else {
    logger.debug(s"Redoing Column Ordering for all datasets in version  $version")
    restorer.restoreAllInVersion(version)
  }


}
