package coop.rchain.rspace.history

import scodec.bits.ByteVector

sealed trait KeyPathNew {
  def key: ByteVector
  def size: Long                        = key.size
  def nonEmpty: Boolean                 = key.nonEmpty
  def head: Byte                        = key.head
  def tailToKey: ByteVector             = key.tail
  def tailToKeyPath: KeyPathNew         = KeyPathNew.create(tailToKey)
  def ++(other: KeyPathNew): KeyPathNew = KeyPathNew.create(key ++ other.key)
}
// TODO: Rename KeyPathNew to KeyPath (after removing implementation [[History.KeyPath]])
object KeyPathNew {
  def create(bv: ByteVector): KeyPathNew =
    new KeyPathNew {
      override def key: ByteVector = bv
    }
  def equal(a: KeyPathNew, b: KeyPathNew): Boolean = a.key == b.key
  def commonPrefix(a: KeyPathNew, b: KeyPathNew): KeyPathNew = {
    val commonPart = (a.key.toArray, b.key.toArray).zipped.takeWhile { case (ll, rr) => ll == rr }
    KeyPathNew.create(ByteVector(commonPart.map(_._1).toArray))
  }

}
