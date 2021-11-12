package de.hpi.data_preparation.socrata.change.temporal_tables.tuple

import de.hpi.data_preparation.socrata.change.{ReservedChangeValues, UpdateChangeCounter}
import de.hpi.data_preparation.socrata.change.temporal_tables.time.TimeInterval
import de.hpi.data_preparation.socrata.change.temporal_tables.tuple.FactLineage.WILDCARD_VALUES
import de.hpi.data_preparation.socrata.io.GLOBAL_CONFIG

import java.time.LocalDate
import java.time.temporal.ChronoUnit
import scala.collection.mutable

@SerialVersionUID(3L)
case class FactLineage(lineage:mutable.TreeMap[LocalDate,Any] = mutable.TreeMap[LocalDate,Any]()) extends Serializable{

  def nonWildcardValueSequenceBefore(trainTimeEnd: LocalDate) = {
    val iterator = lineage.iterator
    var curElem = iterator.nextOption()
    val valueSequence = collection.mutable.ArrayBuffer[Any]()
    while(curElem.isDefined && curElem.get._1.isBefore(trainTimeEnd)){
      val value = curElem.get._2
      if((valueSequence.isEmpty || valueSequence.last != value) && !isWildcard(value))
        valueSequence += value
      curElem = iterator.nextOption()
    }
    valueSequence
  }

  def nonWildcardValueSetBefore(trainTimeEnd: LocalDate) = {
    val iterator = lineage.iterator
    var curElem = iterator.nextOption()
    val valueSet = collection.mutable.HashSet[Any]()
    while(curElem.isDefined && curElem.get._1.isBefore(trainTimeEnd)){
      valueSet.add(curElem.get._2)
      curElem = iterator.nextOption()
    }
    valueSet
  }

  val digitRegex = "[0-9]+"

  def isNumeric = {
    lineage.values.forall(v => FactLineage.isWildcard(v) || v.toString.matches(digitRegex))
  }

  //returns duration in days
  def nonWildcardDuration(timeRangeEnd:LocalDate/*,begin:Option[LocalDate]=None*/) = {
    val withIndex = lineage
      .zipWithIndex
      .toIndexedSeq
    var period:Long = 0
    //if(begin.isDefined) period += Period.between(begin.get,withIndex.head._1)
    withIndex.map{case ((ld,v),i) => {
      val curBegin = ld
      val end = if(i!=withIndex.size-1) withIndex(i+1)._1._1 else timeRangeEnd
      if(!isWildcard(v))
        period = period + (ChronoUnit.DAYS.between(curBegin,end))
    }}
    period
  }

  def toShortString: String = {
    val withoutWIldcard = lineage
      .filter(v => !isWildcard(v._2))
      .toIndexedSeq
      .zipWithIndex
    val withoutDuplicates = withoutWIldcard
      .filter { case ((t, v), i) => i == 0 || v != withoutWIldcard(i - 1)._1._2 }
      .map(_._1)
      .zipWithIndex
    "<" + withoutDuplicates
      .map{case ((t,v),i) => {
        val begin = t
        val end = if(i==withoutDuplicates.size-1) "?" else withoutDuplicates(i+1)._1._1.toString
        begin.toString + "-" + end + ":" +v
      }}//(_._1._2)
      .mkString(",") + ">"
  }

  def projectToTimeRange(timeRangeStart: LocalDate, timeRangeEnd: LocalDate) = {
    val prevStart = lineage.firstKey
    val afterStart = lineage.filter { case (k, v) => !k.isBefore(timeRangeStart) && !k.isAfter(timeRangeEnd) }
    if(afterStart.isEmpty){
      val last = lineage.maxBefore(timeRangeStart).get
      FactLineage(mutable.TreeMap((timeRangeStart,last._2)))
    } else{
      if(afterStart.firstKey!=timeRangeStart){
        if(!lineage.maxBefore(afterStart.firstKey).isDefined){
          println("what?")
          println(this)
          println(this.lineage)
          println(timeRangeStart)
          println(timeRangeEnd)
        }
        val before = lineage.maxBefore(afterStart.firstKey).get
        assert(before._1.isBefore(timeRangeStart))
        afterStart.put(timeRangeStart,before._2)
      }
      assert(afterStart.firstKey==timeRangeStart)
      assert(prevStart == lineage.firstKey)
      FactLineage(afterStart)
    }
  }

  def keepOnlyStandardTimeRange = FactLineage(lineage.filter(!_._1.isAfter(GLOBAL_CONFIG.STANDARD_TIME_FRAME_END)))

  def toSerializationHelper = {
    FactLineageWithHashMap(lineage.toMap)
  }

  def valueAt(ts: LocalDate) = {
    if(lineage.contains(ts))
      lineage(ts)
    else {
      val res = lineage.maxBefore(ts)
      if(res.isDefined) {
        res.get._2
      } else {
        ReservedChangeValues.NOT_EXISTANT_ROW
      }
    }
  }

  override def toString: String = "[" + lineage.values.mkString("|") + "]"

  def firstTimestamp: LocalDate = lineage.firstKey

  def lastTimestamp: LocalDate = lineage.lastKey

  def getValueLineage: mutable.TreeMap[LocalDate, Any] = lineage

  def isWildcard(value: Any) = FactLineage.isWildcard(value)

  def getCompatibleValue(a: Any, b: Any): Any = {
    if(a==b) a else if(isWildcard(a)) b else a
  }

  def valuesInInterval(ti: TimeInterval): IterableOnce[(TimeInterval, Any)] = {
    var toReturn = toIntervalRepresentation
      .withFilter{case (curTi,v) => !curTi.endOrMax.isBefore(ti.begin) && !curTi.begin.isAfter(ti.endOrMax)}
      .map{case (curTi,v) =>
        val end = Seq(curTi.endOrMax,ti.endOrMax).min
        val begin = Seq(curTi.begin,ti.begin).max
        (TimeInterval(begin,Some(`end`)),v)
      }
    if(ti.begin.isBefore(firstTimestamp))
      toReturn += ((TimeInterval(ti.begin,Some(firstTimestamp)),ReservedChangeValues.NOT_EXISTANT_ROW))
    toReturn
  }

  def fromValueLineage[V <: FactLineage](lineage: FactLineage): V = lineage.asInstanceOf[V]

  def fromTimestampToValue[V <: FactLineage](asTree: mutable.TreeMap[LocalDate, Any]): V = FactLineage(asTree).asInstanceOf[V]

  def nonWildCardValues: Iterable[Any] = getValueLineage.values.filter(!isWildcard(_))

  def numValues: Int = lineage.size

  def allTimestamps: Iterable[LocalDate] = lineage.keySet

  def WILDCARDVALUES: Set[Any] = WILDCARD_VALUES


  private def notWCOrEmpty(prevValue1: Option[Any]): Boolean = {
    !prevValue1.isEmpty && !isWildcard(prevValue1.get)
  }

  def allNonWildcardTimestamps: Iterable[LocalDate] = {
    getValueLineage
      .filter(t => !isWildcard(t._2))
      .keySet
  }

  def countChanges(changeCounter: UpdateChangeCounter): (Int,Int) = {
    changeCounter.countChangesForValueLineage(this.lineage,isWildcard)
  }

  def toIntervalRepresentation:mutable.TreeMap[TimeInterval,Any] = {
    val asLineage = getValueLineage.toIndexedSeq
    mutable.TreeMap[TimeInterval,Any]() ++ (0 until asLineage.size).map( i=> {
      val (ts,value) = asLineage(i)
      if(i==asLineage.size-1)
        (TimeInterval(ts,None),value)
      else
        (TimeInterval(ts,Some(asLineage(i+1)._1.minusDays(1))),value)
    })
  }

  def append[V<:FactLineage](y: V): V = {
    assert(lastTimestamp.isBefore(y.firstTimestamp))
    fromTimestampToValue(this.getValueLineage ++ y.getValueLineage)
  }



}
object FactLineage {

  def WILDCARD_VALUES: Set[Any] = Set(ReservedChangeValues.NOT_EXISTANT_DATASET, ReservedChangeValues.NOT_EXISTANT_COL, ReservedChangeValues.NOT_EXISTANT_ROW, ReservedChangeValues.NOT_EXISTANT_CELL, ReservedChangeValues.NOT_KNOWN_DUE_TO_NO_VISIBLE_CHANGE)

  def fromSerializationHelper(valueLineageWithHashMap: FactLineageWithHashMap) = FactLineage(mutable.TreeMap[LocalDate, Any]() ++ valueLineageWithHashMap.lineage)

  def isWildcard(value: Any) = WILDCARD_VALUES.contains(value)
}

