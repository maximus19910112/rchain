package coop.rchain.rspace.nextgenrspace

import coop.rchain.rspace.examples.StringExamples.StringsCaptor
import java.nio.file.{Files, Path}

import cats._
import cats.implicits._
import cats.effect.Sync
import coop.rchain.rspace._
import coop.rchain.rspace.examples.AddressBookExample._
import coop.rchain.rspace.examples.AddressBookExample.implicits._
import coop.rchain.rspace.history.{initialize, Branch, ITrieStore, InMemoryTrieStore, LMDBTrieStore}
import coop.rchain.rspace.internal.{codecGNAT, GNAT}
import coop.rchain.rspace.util._
import coop.rchain.shared.Cell
import coop.rchain.shared.PathOps._
import monix.eval.Coeval
import org.scalatest.BeforeAndAfterAll
import scodec.Codec

import scala.concurrent.ExecutionContext.Implicits.global

import monix.eval.Task

trait StorageExamplesTests[F[_]]
    extends StorageTestsBase[F, Channel, Pattern, Entry, EntriesCaptor] {

  "CORE-365: A joined consume on duplicate channels followed by two produces on that channel" should
    "return a continuation and the produced data" in withTestSpace { (store, space) =>
    for {
      r1 <- space
             .consume(
               List(Channel("friends"), Channel("friends")),
               List(CityMatch(city = "Crystal Lake"), CityMatch(city = "Crystal Lake")),
               new EntriesCaptor,
               persist = false
             )
      _             = r1 shouldBe None
      r2            <- space.produce(Channel("friends"), bob, persist = false)
      _             = r2 shouldBe None
      r3            <- space.produce(Channel("friends"), bob, persist = false)
      _             = r3 shouldBe defined
      _             = runK(r3)
      _             = getK(r3).results shouldBe List(List(bob, bob))
      insertActions <- store.changes().map(collectActions[InsertAction])
      _             = insertActions shouldBe empty
    } yield ()
  }

  "CORE-365: Two produces on the same channel followed by a joined consume on duplicates of that channel" should
    "return a continuation and the produced data" in withTestSpace { (store, space) =>
    for {
      r1 <- space.produce(Channel("friends"), bob, persist = false)
      _  = r1 shouldBe None
      r2 <- space.produce(Channel("friends"), bob, persist = false)
      _  = r2 shouldBe None
      r3 <- space
             .consume(
               List(Channel("friends"), Channel("friends")),
               List(CityMatch(city = "Crystal Lake"), CityMatch(city = "Crystal Lake")),
               new EntriesCaptor,
               persist = false
             )
      _ = r3 shouldBe defined
      _ = runK(r3)
      _ = getK(r3).results shouldBe List(List(bob, bob))
    } yield (store.changes().map(_.isEmpty shouldBe true))
  }

  "CORE-365: A joined consume on duplicate channels given twice followed by three produces" should
    "return a continuation and the produced data" in withTestSpace { (store, space) =>
    for {
      r1 <- space
             .consume(
               List(Channel("colleagues"), Channel("friends"), Channel("friends")),
               List(
                 CityMatch(city = "Crystal Lake"),
                 CityMatch(city = "Crystal Lake"),
                 CityMatch(city = "Crystal Lake")
               ),
               new EntriesCaptor,
               persist = false
             )
      _  = r1 shouldBe None
      r2 <- space.produce(Channel("friends"), bob, persist = false)
      _  = r2 shouldBe None
      r3 <- space.produce(Channel("friends"), bob, persist = false)
      _  = r3 shouldBe None
      r4 <- space.produce(Channel("colleagues"), alice, persist = false)
      _  = r4 shouldBe defined
      _  = runK(r4)
      _  = getK(r4).results shouldBe List(List(alice, bob, bob))
    } yield (store.changes().map(_.isEmpty shouldBe true))

  }

  "CORE-365: A joined consume on multiple duplicate channels followed by the requisite produces" should
    "return a continuation and the produced data" in withTestSpace { (store, space) =>
    for {
      r1 <- space
             .consume(
               List(
                 Channel("family"),
                 Channel("family"),
                 Channel("family"),
                 Channel("family"),
                 Channel("colleagues"),
                 Channel("colleagues"),
                 Channel("colleagues"),
                 Channel("friends"),
                 Channel("friends")
               ),
               List(
                 CityMatch(city = "Herbert"),
                 CityMatch(city = "Herbert"),
                 CityMatch(city = "Herbert"),
                 CityMatch(city = "Herbert"),
                 CityMatch(city = "Crystal Lake"),
                 CityMatch(city = "Crystal Lake"),
                 CityMatch(city = "Crystal Lake"),
                 CityMatch(city = "Crystal Lake"),
                 CityMatch(city = "Crystal Lake")
               ),
               new EntriesCaptor,
               persist = false
             )
      _   = r1 shouldBe None
      r2  <- space.produce(Channel("friends"), bob, persist = false)
      r3  <- space.produce(Channel("family"), carol, persist = false)
      r4  <- space.produce(Channel("colleagues"), alice, persist = false)
      r5  <- space.produce(Channel("friends"), bob, persist = false)
      r6  <- space.produce(Channel("family"), carol, persist = false)
      r7  <- space.produce(Channel("colleagues"), alice, persist = false)
      r8  <- space.produce(Channel("colleagues"), alice, persist = false)
      r9  <- space.produce(Channel("family"), carol, persist = false)
      r10 <- space.produce(Channel("family"), carol, persist = false)

      _ = r2 shouldBe None
      _ = r3 shouldBe None
      _ = r4 shouldBe None
      _ = r5 shouldBe None
      _ = r6 shouldBe None
      _ = r7 shouldBe None
      _ = r8 shouldBe None
      _ = r9 shouldBe None
      _ = r10 shouldBe defined

      _ = runK(r10)
      _ = getK(r10).results shouldBe List(
        List(carol, carol, carol, carol, alice, alice, alice, bob, bob)
      )

    } yield (store.changes().map(_.isEmpty shouldBe true))
  }

  "CORE-365: Multiple produces on multiple duplicate channels followed by the requisite consume" should
    "return a continuation and the produced data" in withTestSpace { (store, space) =>
    for {
      r1 <- space.produce(Channel("friends"), bob, persist = false)
      r2 <- space.produce(Channel("family"), carol, persist = false)
      r3 <- space.produce(Channel("colleagues"), alice, persist = false)
      r4 <- space.produce(Channel("friends"), bob, persist = false)
      r5 <- space.produce(Channel("family"), carol, persist = false)
      r6 <- space.produce(Channel("colleagues"), alice, persist = false)
      r7 <- space.produce(Channel("colleagues"), alice, persist = false)
      r8 <- space.produce(Channel("family"), carol, persist = false)
      r9 <- space.produce(Channel("family"), carol, persist = false)

      _ = r1 shouldBe None
      _ = r2 shouldBe None
      _ = r3 shouldBe None
      _ = r4 shouldBe None
      _ = r5 shouldBe None
      _ = r6 shouldBe None
      _ = r7 shouldBe None
      _ = r8 shouldBe None
      _ = r9 shouldBe None

      r10 <- space
              .consume(
                List(
                  Channel("family"),
                  Channel("family"),
                  Channel("family"),
                  Channel("family"),
                  Channel("colleagues"),
                  Channel("colleagues"),
                  Channel("colleagues"),
                  Channel("friends"),
                  Channel("friends")
                ),
                List(
                  CityMatch(city = "Herbert"),
                  CityMatch(city = "Herbert"),
                  CityMatch(city = "Herbert"),
                  CityMatch(city = "Herbert"),
                  CityMatch(city = "Crystal Lake"),
                  CityMatch(city = "Crystal Lake"),
                  CityMatch(city = "Crystal Lake"),
                  CityMatch(city = "Crystal Lake"),
                  CityMatch(city = "Crystal Lake")
                ),
                new EntriesCaptor,
                persist = false
              )

      _ = r10 shouldBe defined
      _ = runK(r10)
      _ = getK(r10).results shouldBe List(
        List(carol, carol, carol, carol, alice, alice, alice, bob, bob)
      )
    } yield (store.changes().map(_.isEmpty shouldBe true))
  }

  "CORE-365: A joined consume on multiple mixed up duplicate channels followed by the requisite produces" should
    "return a continuation and the produced data" in withTestSpace { (store, space) =>
    for {
      r1 <- space
             .consume(
               List(
                 Channel("family"),
                 Channel("colleagues"),
                 Channel("family"),
                 Channel("friends"),
                 Channel("friends"),
                 Channel("family"),
                 Channel("colleagues"),
                 Channel("colleagues"),
                 Channel("family")
               ),
               List(
                 CityMatch(city = "Herbert"),
                 CityMatch(city = "Crystal Lake"),
                 CityMatch(city = "Herbert"),
                 CityMatch(city = "Crystal Lake"),
                 CityMatch(city = "Crystal Lake"),
                 CityMatch(city = "Herbert"),
                 CityMatch(city = "Crystal Lake"),
                 CityMatch(city = "Crystal Lake"),
                 CityMatch(city = "Herbert")
               ),
               new EntriesCaptor,
               persist = false
             )

      _ = r1 shouldBe None

      r2  <- space.produce(Channel("friends"), bob, persist = false)
      r3  <- space.produce(Channel("family"), carol, persist = false)
      r4  <- space.produce(Channel("colleagues"), alice, persist = false)
      r5  <- space.produce(Channel("friends"), bob, persist = false)
      r6  <- space.produce(Channel("family"), carol, persist = false)
      r7  <- space.produce(Channel("colleagues"), alice, persist = false)
      r8  <- space.produce(Channel("colleagues"), alice, persist = false)
      r9  <- space.produce(Channel("family"), carol, persist = false)
      r10 <- space.produce(Channel("family"), carol, persist = false)

      _ = r2 shouldBe None
      _ = r3 shouldBe None
      _ = r4 shouldBe None
      _ = r5 shouldBe None
      _ = r6 shouldBe None
      _ = r7 shouldBe None
      _ = r8 shouldBe None
      _ = r9 shouldBe None
      _ = r10 shouldBe defined

      _ = runK(r10)
      _ = getK(r10).results shouldBe List(
        List(carol, alice, carol, bob, bob, carol, alice, alice, carol)
      )
    } yield (store.changes().map(_.isEmpty shouldBe true))
  }
}

abstract class InMemoryHotStoreStorageExamplesTestsBase[F[_]]
    extends StorageTestsBase[F, Channel, Pattern, Entry, EntriesCaptor] {

  override def withTestSpace[R](f: (ST, T) => F[R]): R = {

    /*
    implicit val cg: Codec[GNAT[Channel, Pattern, Entry, EntriesCaptor]] = codecGNAT(
      serializeChannel.toCodec,
      serializePattern.toCodec,
      serializeInfo.toCodec,
      serializeEntriesCaptor.toCodec
    )*/

    val branch = Branch("inmem")

    run(for {
      historyState <- Cell.refCell[F, Cache[Channel, Pattern, Entry, EntriesCaptor]](
                       Cache[Channel, Pattern, Entry, EntriesCaptor]()
                     )
      historyReader = {
        implicit val h = historyState
        new History[F, Channel, Pattern, Entry, EntriesCaptor]
      }
      cache <- Cell.refCell[F, Cache[Channel, Pattern, Entry, EntriesCaptor]](
                Cache[Channel, Pattern, Entry, EntriesCaptor]()
              )
      testStore = {
        implicit val hr = historyReader
        implicit val c  = cache
        HotStore.inMem[F, Channel, Pattern, Entry, EntriesCaptor]
      }
      testSpace <- RSpace.create[F, Channel, Pattern, Entry, Entry, EntriesCaptor](
                    testStore,
                    branch
                  )
      res <- f(testStore, testSpace)
    } yield { res })
  }
}

class InMemoryHotStoreStorageExamplesTests
    extends InMemoryHotStoreStorageExamplesTestsBase[Task]
    with TaskTests[Channel, Pattern, Entry, Entry, EntriesCaptor]
    with StorageExamplesTests[Task]