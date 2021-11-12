package de.hpi.data_preparation.socrata.json.helper

import de.hpi.data_preparation.socrata.metadata.custom.schemaHistory.AttributeLineageWithHashMap
import de.hpi.data_preparation.socrata.{JsonReadable, JsonWritable}

case class TemporalSchemaHelper(val id:String,val attributes:collection.IndexedSeq[AttributeLineageWithHashMap]) extends JsonWritable[TemporalSchemaHelper]

object TemporalSchemaHelper extends JsonReadable[TemporalSchemaHelper]
