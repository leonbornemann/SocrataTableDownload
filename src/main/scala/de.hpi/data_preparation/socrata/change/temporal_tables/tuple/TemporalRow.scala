package de.hpi.data_preparation.socrata.change.temporal_tables.tuple

class TemporalRow(val entityID:Long,val fields:collection.IndexedSeq[FactLineage]) extends Serializable{
  private def serialVersionUID = 42L

}
object TemporalRow{
}
