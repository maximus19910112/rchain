package coop.rchain.rspace.history

import scodec.bits.ByteVector

sealed trait KeyPathNew {
  def value: ByteVector
  def size: Long                        = value.size
  def nonEmpty: Boolean                 = value.nonEmpty
  def head: Byte                        = value.head
  def tailToValue: ByteVector           = value.tail
  def tail: KeyPathNew                  = KeyPathNew.create(tailToValue)
  def ++(other: KeyPathNew): KeyPathNew = KeyPathNew.create(value ++ other.value)
  def ==(other: KeyPathNew): Boolean    = value == other.value
}
// TODO: Rename KeyPathNew to KeyPath (after removing implementation [[History.KeyPath]])
object KeyPathNew {
  def create(bv: ByteVector): KeyPathNew =
    new KeyPathNew {
      override def value: ByteVector = bv
    }
  def equal(a: KeyPathNew, b: KeyPathNew): Boolean = a.value == b.value
  def commonPath(a: KeyPathNew, b: KeyPathNew): KeyPathNew = {
    val commonPart = (a.value.toArray, b.value.toArray).zipped.takeWhile {
      case (ll, rr) => ll == rr
    }
    val commonPartBV = ByteVector(commonPart.map(_._1).toArray)
    KeyPathNew.create(commonPartBV)
  }

  val empty = KeyPathNew.create(ByteVector.empty)
}
