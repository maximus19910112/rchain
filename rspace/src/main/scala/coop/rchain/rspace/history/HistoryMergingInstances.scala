package coop.rchain.rspace.history

import cats.effect.{Concurrent, Sync}
import cats.syntax.all._
import cats.{Applicative, FlatMap, Parallel}
import coop.rchain.rspace.hashing.Blake2b256Hash
import coop.rchain.rspace.history.History._
import coop.rchain.rspace.serializers.ScodecSerialize.{
  codecPointerBlock,
  codecSkip,
  codecTrie,
  RichAttempt
}
import coop.rchain.shared.Base16
import coop.rchain.shared.syntax._
import scodec.bits.{BitVector, ByteVector}

import scala.Function.tupled
import scala.Ordering.Implicits.seqDerivedOrdering
import scala.collection.concurrent.TrieMap

object HistoryMergingInstances {

  type Index            = Byte
  type LastModification = (KeyPath, Trie)
  type SubtrieAtIndex   = (Index, KeyPath, NonEmptyTriePointer)

  def MalformedTrieError = new RuntimeException("malformed trie")

  val emptyRoot: Trie               = EmptyTrie
  private[this] def encodeEmptyRoot = codecTrie.encode(emptyRoot).getUnsafe.toByteVector
  val emptyRootHash: Blake2b256Hash = Blake2b256Hash.create(encodeEmptyRoot)

  // this mapping is kept explicit on purpose
  @inline
  private[history] def toInt(b: Byte): Int =
    java.lang.Byte.toUnsignedInt(b)

  // this mapping is kept explicit on purpose
  @inline
  private[history] def toByte(i: Int): Byte =
    i.toByte

  def commonPrefix(l: KeyPath, r: KeyPath): KeyPath =
    (l.view, r.view).zipped.takeWhile { case (ll, rr) => ll == rr }.map(_._1).toSeq

  final case class CachingHistoryStore[F[_]: Sync](historyStore: HistoryStore[F])
      extends HistoryStore[F] {
    private[rspace] val cache: TrieMap[Blake2b256Hash, Trie] = TrieMap.empty

    override def put(tries: List[Trie]): F[Unit] =
      Sync[F].delay {
        tries.foreach { t =>
          cache.put(Trie.hash(t), t)
        }
      }

    override def get(key: Blake2b256Hash): F[Trie] =
      for {
        maybeValue <- Sync[F].delay { cache.get(key) }
        result <- maybeValue match {
                   case None    => historyStore.get(key)
                   case Some(v) => Applicative[F].pure(v)
                 }
      } yield result

    def clear(): F[Unit] = Sync[F].delay {
      cache.clear()
    }

    def drop(tries: List[Trie]): F[Unit] =
      Sync[F].delay {
        tries.foreach { t =>
          cache.remove(Trie.hash(t))
        }
      }

    def commit(key: Blake2b256Hash): F[Unit] = {
      def getValue(key: Blake2b256Hash): List[Trie] =
        cache.get(key).toList // if a key exists in cache - we want to process it

      def extractRefs(t: Trie): Seq[Blake2b256Hash] =
        t match {
          case pb: PointerBlock =>
            pb.toVector.toList.filter(_ != EmptyPointer).flatMap {
              case v: SkipPointer => v.hash :: Nil
              case v: NodePointer => v.hash :: Nil
              case _              => Nil
            }
          case Skip(_, LeafPointer(_))    => Nil
          case Skip(_, NodePointer(hash)) => hash :: Nil
          case EmptyTrie                  => Nil
        }

      def go(keys: List[Blake2b256Hash]): F[Either[List[Blake2b256Hash], Unit]] =
        if (keys.isEmpty) Sync[F].pure(().asRight)
        else {
          val head :: rest = keys
          for {
            ts   <- Sync[F].delay { getValue(head) }
            _    <- historyStore.put(ts)
            _    <- Sync[F].delay { cache.remove(head) }
            refs = ts.flatMap(extractRefs)
          } yield (refs ++ rest).asLeft
        }
      Sync[F].tailRecM(key :: Nil)(go)
    }

  }
}

/*
 * Type definitions for Merkle Trie implementation (History)
 */

sealed trait Trie

sealed trait NonEmptyTrie extends Trie

case object EmptyTrie extends Trie

final case class Skip(affix: ByteVector, ptr: ValuePointer) extends NonEmptyTrie {
  lazy val encoded: BitVector = codecTrie.encode(this).getUnsafe

  lazy val hash: Blake2b256Hash = Blake2b256Hash.create(encoded.toByteVector)

  override def toString: String =
    s"Skip(${hash}, ${affix.toHex}\n  ${ptr})"
}

final case class PointerBlock private (toVector: Vector[TriePointer]) extends NonEmptyTrie {
  def updated(tuples: List[(Int, TriePointer)]): PointerBlock =
    new PointerBlock(tuples.foldLeft(toVector) { (vec, curr) =>
      vec.updated(curr._1, curr._2)
    })

  def countNonEmpty: Int = toVector.count(_ != EmptyPointer)

  lazy val encoded: BitVector = codecTrie.encode(this).getUnsafe

  lazy val hash: Blake2b256Hash = Blake2b256Hash.create(encoded.toByteVector)

  override def toString: String = {
    // TODO: this is difficult to visualize, maybe XML representation would be useful?
    val pbs =
      toVector.zipWithIndex
        .filter { case (v, _) => v != EmptyPointer }
        .map { case (v, n) => s"<$v, ${Base16.encode(Array(n.toByte))}>" }
        .mkString(",\n  ")
    s"PB(${hash}\n  $pbs)"
  }
}

object Trie {

  /**
    * Creates hash of Merkle Trie
    *
    * TODO: Fix encoding to use codec for the whole [[Trie]] and not for specific inherited variant.
    */
  def hash(trie: Trie): Blake2b256Hash =
    trie match {
      case pb: PointerBlock  => pb.hash
      case s: Skip           => s.hash
      case _: EmptyTrie.type => HistoryMergingInstances.emptyRootHash
    }
}

object PointerBlock {
  val length = 256

  val empty: PointerBlock = new PointerBlock(Vector.fill(length)(EmptyPointer))

  def apply(first: (Int, TriePointer), second: (Int, TriePointer)): PointerBlock =
    PointerBlock.empty.updated(List(first, second))

  def unapply(arg: PointerBlock): Option[Vector[TriePointer]] = Option(arg.toVector)
}

/*
 * Trie pointer definitions
 */

sealed trait TriePointer

sealed trait NonEmptyTriePointer extends TriePointer {
  def hash: Blake2b256Hash
}

case object EmptyPointer extends TriePointer

sealed trait ValuePointer extends NonEmptyTriePointer

final case class LeafPointer(hash: Blake2b256Hash) extends ValuePointer

final case class SkipPointer(hash: Blake2b256Hash) extends NonEmptyTriePointer

final case class NodePointer(hash: Blake2b256Hash) extends ValuePointer

/*
 * Trie path used as a helper for traversal in History implementation
 */

final case class TriePath(nodes: Vector[Trie], conflicting: Option[Trie], edges: KeyPath) {
  def append(affix: KeyPath, t: Trie): TriePath =
    this.copy(nodes = this.nodes :+ t, edges = this.edges ++ affix)
}

object TriePath {
  def empty: TriePath = TriePath(Vector(), None, Nil)
}
