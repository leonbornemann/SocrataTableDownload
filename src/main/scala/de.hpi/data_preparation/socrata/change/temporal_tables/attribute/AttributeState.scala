package de.hpi.data_preparation.socrata.change.temporal_tables.attribute

import de.hpi.data_preparation.socrata.change.ReservedChangeValues
import de.hpi.data_preparation.socrata.simplified.Attribute

@SerialVersionUID(3L)
case class AttributeState(attr:Option[Attribute]) extends Serializable{
  def exists: Boolean = attr.isDefined

  def isNE: Boolean = !attr.isDefined

  def displayName = if(attr.isDefined) attr.get.name else ReservedChangeValues.NE_DISPLAY

}

object AttributeState {
  val NON_EXISTANT = AttributeState(None)
}
