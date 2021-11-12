package de.hpi.data_preparation.socrata.diff.semantic

import de.hpi.data_preparation.socrata.DatasetInstance

import scala.collection.mutable

class DatasetMatching() {
  val deletes = mutable.HashSet[DatasetInstance]()
  val matchings = mutable.HashMap[DatasetInstance,DatasetInstance]()
  val inserts = mutable.HashSet[DatasetInstance]()


}
