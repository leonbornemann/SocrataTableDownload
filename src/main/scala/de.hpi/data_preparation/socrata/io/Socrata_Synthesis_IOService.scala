package de.hpi.data_preparation.socrata.io

import com.typesafe.scalalogging.StrictLogging

import java.io.File
import java.time.LocalDate

object Socrata_Synthesis_IOService extends StrictLogging {
  def DB_SYNTHESIS_DIR = socrataDir + "/db_synthesis"

  //statistics and reporting:
  def STATISTICS_DIR = DB_SYNTHESIS_DIR + "/statistics/"

  def WORKING_DIR = DB_SYNTHESIS_DIR + "/workingDir/"

  //decomposition:
  def DECOMPOSTION_DIR(subdomain: String) = DB_SYNTHESIS_DIR + s"/decomposition/$subdomain/"

  def FDDIR(subdomain: String) = DECOMPOSTION_DIR(subdomain) + "/fds/"

  def COLID_FDDIR(subdomain: String) = DECOMPOSTION_DIR(subdomain) + "/fds_colID/"

  def BCNF_SCHEMA_FILE(subdomain: String) = DECOMPOSTION_DIR(subdomain) + "/bcnfSchemata/"

  //association schema and input
  def ASSOCIATION_SCHEMA_DIR(subdomain: String) = DECOMPOSTION_DIR(subdomain) + "/associationSchemata/"

  def OPTIMIZATION_INPUT_DIR(subdomain: String) = DB_SYNTHESIS_DIR + s"/input/$subdomain/"

  def OPTIMIZATION_INPUT_ASSOCIATION_SKETCH_DIR(subdomain: String) = OPTIMIZATION_INPUT_DIR(subdomain) + "/associationSketches/"

  def OPTIMIZATION_INPUT_ASSOCIATION_DIR(subdomain: String) = OPTIMIZATION_INPUT_DIR(subdomain) + "/associations/"

  def OPTIMIZATION_INPUT_BCNF_DIR(subdomain: String) = OPTIMIZATION_INPUT_DIR(subdomain) + "/BCNF/"

  def OPTIMIZATION_INPUT_FULL_TIME_RANGE_ASSOCIATION_DIR(subdomain: String) = OPTIMIZATION_INPUT_DIR(subdomain) + "/FullTimeRangeAssociations/"

  def OPTIMIZATION_INPUT_FULL_TIME_RANGE_ASSOCIATION_SKETCH_DIR(subdomain: String) = OPTIMIZATION_INPUT_DIR(subdomain) + "/FullTimeRangeAssociationSketches/"

  def OPTIMIZATION_INPUT_FACTLOOKUP_DIR(viewID: String, subdomain: String) = OPTIMIZATION_INPUT_DIR(subdomain) + s"/factLookupTables/$viewID/"

  def getStatisticsDir(subdomain: String, originalID: String) = createParentDirs(new File(s"$STATISTICS_DIR/$subdomain/$originalID/"))

  def createParentDirs(f: File) = {
    val parent = f.getParentFile
    parent.mkdirs()
    f
  }

  def dateToStr(date: LocalDate) = GLOBAL_CONFIG.dateTimeFormatter.format(date)

  def socrataDir = Socrata_IOService.socrataDir
}
