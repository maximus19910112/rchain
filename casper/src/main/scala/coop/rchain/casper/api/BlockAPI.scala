package coop.rchain.casper.api

import cats.Monad
import cats.implicits._
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import coop.rchain.casper.api.BlockAPI.getBlockInfo
import coop.rchain.casper.protocol._
import coop.rchain.casper.util.ProtoUtil
import coop.rchain.casper.{BlockDag, MultiParentCasper, PrettyPrinter}
import coop.rchain.crypto.codec.Base16

import scala.annotation.tailrec

object BlockAPI {
  def createBlock[F[_]: Monad: MultiParentCasper]: F[MaybeBlockMessage] =
    MultiParentCasper[F].createBlock.map(MaybeBlockMessage.apply)

  def addBlock[F[_]: Monad: MultiParentCasper](b: BlockMessage): F[Empty] =
    MultiParentCasper[F].addBlock(b).map(_ => Empty())

  def getBlocksResponse[F[_]: Monad: MultiParentCasper]: F[BlocksResponse] =
    for {
      estimates <- MultiParentCasper[F].estimator
      dag       <- MultiParentCasper[F].blockDag
      mainChain: IndexedSeq[BlockMessage] = getMainChain[F](estimates.head,
                                                            dag,
                                                            IndexedSeq.empty[BlockMessage])
      blockInfos <- mainChain.toList.traverse(getBlockInfo[F])
    } yield BlocksResponse(status = "Success", blocks = blockInfos)

  @tailrec
  private def getMainChain[F[_]: Monad: MultiParentCasper](
      estimate: BlockMessage,
      dag: BlockDag,
      acc: IndexedSeq[BlockMessage]): IndexedSeq[BlockMessage] = {
    val parentsHashes    = ProtoUtil.parents(estimate)
    val mainParentHash   = parentsHashes.head
    val maybeNewEstimate = dag.blockLookup.get(mainParentHash)
    maybeNewEstimate match {
      case Some(newEstimate) =>
        getMainChain[F](newEstimate, dag, acc :+ estimate)
      case None =>
        acc
    }
  }

  def getBlockQueryResponse[F[_]: Monad: MultiParentCasper](q: BlockQuery): F[BlockQueryResponse] =
    for {
      dag        <- MultiParentCasper[F].blockDag
      maybeBlock = getBlock[F](q, dag)
      blockQueryResponse <- maybeBlock match {
                             case Some(block) => {
                               for {
                                 blockInfo <- getBlockInfo[F](block)
                               } yield
                                 BlockQueryResponse(status = "Success", blockInfo = Some(blockInfo))
                             }
                             case None =>
                               BlockQueryResponse(
                                 status = s"Error: Failure to find block with hash ${q.hash}")
                                 .pure[F]
                           }
    } yield blockQueryResponse

  private def getBlockInfo[F[_]: Monad: MultiParentCasper](block: BlockMessage): F[BlockInfo] =
    for {
      parents <- block.header.fold(Seq.empty[ByteString])(_.parentsHashList).pure[F]
      tsHash <- {
        val ps = block.body.flatMap(_.postState)
        ps.fold(ByteString.EMPTY)(_.tuplespace).pure[F]
      }
      tsDesc <- MultiParentCasper[F]
                 .tsCheckpoint(tsHash)
                 .map(maybeCheckPoint => {
                   maybeCheckPoint
                     .map(checkpoint => {
                       val ts     = checkpoint.toTuplespace
                       val result = ts.storageRepr
                       ts.delete()
                       result
                     })
                     .getOrElse(s"Tuplespace hash ${Base16.encode(tsHash.toByteArray)} not found!")
                 })
    } yield {
      BlockInfo(
        blockHash = PrettyPrinter.buildStringNoLimit(block.blockHash),
        blockNumber = ProtoUtil.blockNumber(block),
        parentsHashList = parents.map(PrettyPrinter.buildStringNoLimit),
        tupleSpaceDump = tsDesc
      )
    }

  private def getBlock[F[_]: Monad: MultiParentCasper](q: BlockQuery,
                                                       dag: BlockDag): Option[BlockMessage] = {
    val fullHash = dag.blockLookup.keys.find(h => {
      Base16.encode(h.toByteArray).startsWith(q.hash)
    })
    fullHash.map(h => dag.blockLookup(h))
  }
}
