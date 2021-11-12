package de.hpi.data_preparation.socrata.change.temporal_tables

import java.io.{File, FileOutputStream, ObjectOutputStream}

trait BinarySerializable extends Serializable {

  private def serialVersionUID = 6529685098267757690L

  def writeToBinaryFile(f: File) = {
    val o = new ObjectOutputStream(new FileOutputStream(f))
    o.writeObject(this)
    o.close()
  }
}
