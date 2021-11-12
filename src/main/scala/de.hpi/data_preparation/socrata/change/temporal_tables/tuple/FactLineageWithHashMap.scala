package de.hpi.data_preparation.socrata.change.temporal_tables.tuple

import de.hpi.data_preparation.socrata.{JsonReadable, JsonWritable}

import java.time.LocalDate
/***
 * Enables easy serialization as json
 * @param lineage
 */
case class FactLineageWithHashMap(lineage: Map[LocalDate, Any]) extends JsonWritable[FactLineageWithHashMap]{
  def toRoleLineage = FactLineage.fromSerializationHelper(this)

}

object RoleLineageWithHashMap extends JsonReadable[FactLineageWithHashMap]
