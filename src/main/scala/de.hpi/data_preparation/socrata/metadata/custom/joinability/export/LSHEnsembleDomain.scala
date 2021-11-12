package de.hpi.data_preparation.socrata.metadata.custom.joinability.`export`

import de.hpi.data_preparation.socrata.JsonWritable

case class LSHEnsembleDomain(id: String, version: String, attrName: String, values: Set[String]) extends JsonWritable[LSHEnsembleDomain]{

}
