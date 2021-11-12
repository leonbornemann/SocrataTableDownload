package de.hpi.data_preparation.socrata.json.custom_serializer

import de.hpi.data_preparation.socrata
import de.hpi.data_preparation.socrata.DatasetInstance
import de.hpi.data_preparation.socrata.io.Socrata_IOService
import org.json4s.CustomKeySerializer

import java.time.LocalDate

case object DatasetInstanceKeySerializer extends CustomKeySerializer[DatasetInstance](format => ( {
  case s: String => socrata.DatasetInstance(s.split(",")(0), LocalDate.parse(s.split(",")(1), Socrata_IOService.dateTimeFormatter))
}, {
  case i: DatasetInstance => s"${i.id},${i.date.format(Socrata_IOService.dateTimeFormatter)}"
}
)) {

}
