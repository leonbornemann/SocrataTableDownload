package de.hpi.data_preparation.socrata

import de.hpi.data_preparation.socrata.io.Socrata_IOService
import de.hpi.data_preparation.socrata.metadata.DatasetMetadata

import java.time.LocalDate

case class DatasetInstance(val id: String, val date: LocalDate) extends Serializable {

  var datasetMetadata: Option[DatasetMetadata] = None

  def loadMetadata(): Unit = {
    if (!datasetMetadata.isDefined)
      datasetMetadata = Socrata_IOService.getMetadataForDataset(date, id)
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[DatasetInstance]

  override def equals(other: Any): Boolean = other match {
    case that: DatasetInstance =>
      (that canEqual this) &&
        id == that.id &&
        date == that.date
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id, date)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }


  override def toString = s"Dataset($id, $date)"
}
