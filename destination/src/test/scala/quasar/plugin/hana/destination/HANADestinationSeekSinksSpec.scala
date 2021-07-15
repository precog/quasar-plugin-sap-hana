/*
 * Copyright 2020 Precog Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.plugin.hana.destination

import slamdata.Predef._

import quasar.plugin.hana._

import quasar.api.Column
import quasar.api.push.{OffsetKey, PushColumns}
import quasar.api.resource.{ResourceName, ResourcePath}
import quasar.connector._
import quasar.connector.destination.{WriteMode => QWriteMode, _}
import quasar.contrib.scalaz.MonadError_
import quasar.lib.jdbc.Ident

import argonaut._, Argonaut._, ArgonautScalaz._

import cats.Eq
import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.util.Read

import java.lang.CharSequence
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

import fs2.{Chunk, Pull, Stream}

import org.slf4s.Logging
import org.specs2.execute.AsResult
import org.specs2.specification.core.{Fragment => SFragment}

import shapeless._
import shapeless.ops.hlist.{Mapper, ToList}
import shapeless.ops.record.{Keys, Values}
import shapeless.record._
import shapeless.syntax.singleton._

import scalaz.syntax.show._

object HANADestinationSeekSinksSpec extends TestHarness with Logging {

  skipAllIf(TestUrl.isEmpty)

  def encodedUri =
    s"jdbc:sap://${URLEncoder.encode(TestUrl.drop(11), StandardCharsets.UTF_8.toString)}"

  def config(schema: Option[String] = None): Json = {
    val connectionJson =
      ("jdbcUrl" := encodedUri) ->:
      ("parameters" := List[Json]()) ->:
      ("maxConcurrentcy" := jNull) ->:
      ("maxLifetime" := jNull) ->:
      jEmptyObject

    ("connection" := connectionJson) ->:
    ("schema" := schema) ->:
    ("writeMode" := "replace") ->:
    ("retryTransactionTimeoutMs" := 0) ->:
    ("maxTransactionReattempts" := 0) ->:
    jEmptyObject
  }

  val Mod = HANADestinationModule

  "seek sinks (upsert and append)" should {
    val XStringCol = Column("x", HANAType.VARCHAR(512))
    "write after commit" >> Consumer.appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(
            ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
            ("x" ->> "baz") :: ("y" ->> "quux") :: HNil)),
          UpsertEvent.Commit("commit1"))
      for {
        table <- freshTableName
        (values, offsets) <- consumer(table, toOpt(XStringCol), QWriteMode.Replace, events)
      } yield {
        values must containTheSameElementsAs(List(
          "foo" :: "bar" :: HNil,
          "baz" :: "quux" :: HNil))
      }
    }
    "write two chunks with a single commit" >> Consumer.appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(
            ("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Create(List(
            ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
          UpsertEvent.Commit("commit1"))

      for {
        tbl <- freshTableName
        (values, offsets) <- consumer(tbl, toOpt(XStringCol), QWriteMode.Replace, events)
        } yield {
          values must containTheSameElementsAs(List(
            "foo" :: "bar" :: HNil,
            "baz" :: "qux" :: HNil))

          offsets must containTheSameElementsAs(List(OffsetKey.Actual.string("commit1")))
        }
    }

    "not write without a commit" >> Consumer.appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Create(List(("x" ->> "quz") :: ("y" ->> "corge") :: HNil)),
          UpsertEvent.Commit("commit1"),
          UpsertEvent.Create(List(("x" ->> "baz") :: ("y" ->> "qux") :: HNil)))

      for {
        tbl <- freshTableName
        (values, offsets) <- consumer(tbl, toOpt(XStringCol), QWriteMode.Replace, events)
        } yield {
          values must containTheSameElementsAs(List(
            "foo" :: "bar" :: HNil,
            "quz" :: "corge" :: HNil))
          offsets must containTheSameElementsAs(List(OffsetKey.Actual.string("commit1")))
        }
    }

    "commit twice in a row" >> Consumer.appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Commit("commit1"),
          UpsertEvent.Commit("commit2"))

      for {
        tbl <- freshTableName
        (values, offsets) <- consumer(tbl, toOpt(XStringCol), QWriteMode.Replace, events)
        } yield {
          values must containTheSameElementsAs(List("foo" :: "bar" :: HNil))
          offsets must containTheSameElementsAs(List(
            OffsetKey.Actual.string("commit1"),
            OffsetKey.Actual.string("commit2")))
        }
    }

    "upsert updates rows with string typed primary key on append" >> {
      Consumer.upsert[String :: String :: HNil]().use { consumer =>
        val events =
          Stream(
            UpsertEvent.Create(
              List(
                ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
                ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
            UpsertEvent.Commit("commit1"))

        val append =
          Stream(
            UpsertEvent.Delete(Ids.StringIds(List("foo"))),
            UpsertEvent.Create(
              List(
                ("x" ->> "foo") :: ("y" ->> "check0") :: HNil,
                ("x" ->> "bar") :: ("y" ->> "check1") :: HNil)),
            UpsertEvent.Commit("commit2"))

        for {
          tbl <- freshTableName
          (values1, offsets1) <- consumer(tbl, Some(XStringCol), QWriteMode.Replace, events)
          (values2, offsets2) <- consumer(tbl, Some(XStringCol), QWriteMode.Append, append)
        } yield {
          offsets1 must containTheSameElementsAs(List(OffsetKey.Actual.string("commit1")))
          offsets2 must containTheSameElementsAs(List(OffsetKey.Actual.string("commit2")))
          values1 must containTheSameElementsAs(List("foo" :: "bar" :: HNil, "baz" :: "qux" :: HNil))
          values2 must containTheSameElementsAs(List("baz" :: "qux" :: HNil, "foo" :: "check0" :: HNil, "bar" :: "check1" :: HNil))
        }
      }
    }

    "upsert deletes rows with long typed primary key on append" >> {
      Consumer.upsert[Int :: String :: HNil]().use { consumer =>
        val events =
          Stream(
            UpsertEvent.Create(
              List(
                ("x" ->> 40) :: ("y" ->> "bar") :: HNil,
                ("x" ->> 42) :: ("y" ->> "qux") :: HNil)),
            UpsertEvent.Commit("commit1"))

        val append =
          Stream(
            UpsertEvent.Delete(Ids.LongIds(List(40))),
            UpsertEvent.Create(
              List(
                ("x" ->> 40) :: ("y" ->> "check") :: HNil)),
            UpsertEvent.Commit("commit2"))

        for {
          tbl <- freshTableName
          (values1, offsets1) <- consumer(tbl, Some(Column("x", HANAType.INTEGER)), QWriteMode.Replace, events)
          (values2, offsets2) <- consumer(tbl, Some(Column("x", HANAType.INTEGER)), QWriteMode.Append, append)
        } yield {
          Set(values1:_*) must_== Set(40 :: "bar" :: HNil, 42 :: "qux" :: HNil)
          Set(values2:_*) must_== Set(40 :: "check" :: HNil, 42 :: "qux" :: HNil)
          offsets1 must_== List(OffsetKey.Actual.string("commit1"))
          offsets2 must_== List(OffsetKey.Actual.string("commit2"))
        }
      }
    }

    "upsert empty deletes without failing" >> {
      Consumer.upsert[String :: String :: HNil]().use { consumer =>
        val events =
          Stream(
            UpsertEvent.Create(
              List(
                ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
                ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
            UpsertEvent.Commit("commit1"),
            UpsertEvent.Delete(Ids.StringIds(List())),
            UpsertEvent.Commit("commit2"))

        for {
          tbl <- freshTableName
          (values, offsets) <- consumer(tbl, Some(XStringCol), QWriteMode.Replace, events)
        } yield {
          values must containTheSameElementsAs(List(
            "foo" :: "bar" :: HNil,
            "baz" :: "qux" :: HNil))

          offsets must containTheSameElementsAs(List(
            OffsetKey.Actual.string("commit1"),
            OffsetKey.Actual.string("commit2")))
        }
      }
    }

    "creates table and then appends" >> Consumer.appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events1 =
        Stream(
          UpsertEvent.Create(List(("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Commit("commit1"))

      val events2 =
        Stream(
          UpsertEvent.Create(List(("x" ->> "bar") :: ("y" ->> "qux") :: HNil)),
          UpsertEvent.Commit("commit2"))

      for {
        tbl <- freshTableName

        _ <- consumer(tbl, toOpt(XStringCol), QWriteMode.Replace, events1)

        (values, _) <- consumer(tbl, Some(XStringCol), QWriteMode.Append, events2)
        } yield {
          values must containTheSameElementsAs(List(
            "foo" :: "bar" :: HNil,
            "bar" :: "qux" :: HNil))
        }
    }
  }

  implicit val CS: ContextShift[IO] = IO.contextShift(global)

  implicit val TM: Timer[IO] = IO.timer(global)

  implicit val MRE: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  val freshTableName: IO[String] =
    IO(s"hanatest${Random.alphanumeric.take(6).mkString.toLowerCase}")

  private def dest(cfg: Json): Resource[IO, Destination[IO]] =
    Mod.destination[IO](cfg, _ => _ => Stream.empty, _ => None.pure[IO]) flatMap {
      case Left(err) => Resource.eval(IO.raiseError(new RuntimeException(err.shows)))
      case Right(d) => d.pure[Resource[IO, *]]
    }

  trait Consumer[V <: HList]{
    def apply[R <: HList, K <: HList, T <: HList, S <: HList](
        table: String,
        idColumn: Option[Column[HANAType]],
        writeMode: QWriteMode,
        records: Stream[IO, UpsertEvent[R]])(
        implicit
        read: Read[V],
        keys: Keys.Aux[R, K],
        values: Values.Aux[R, V],
        getTypes: Mapper.Aux[columnType.type, V, T],
        rrow: Mapper.Aux[render.type, V, S],
        ktl: ToList[K, String],
        vtl: ToList[S, String],
        ttl: ToList[T, HANAType])
        : IO[(List[V], List[OffsetKey.Actual[String]])]
  }

  object Consumer {
    def upsert[V <: HList](cfg: Json = config()): Resource[IO, Consumer[V]] = {
      val rsink: Resource[IO, ResultSink.UpsertSink[IO, HANAType, CharSequence]] =
        dest(cfg) evalMap { dst =>
          val optSink = dst.sinks.toList.collectFirst { case c @ ResultSink.UpsertSink(_) => c }
          optSink match {
            case Some(s) => s.asInstanceOf[ResultSink.UpsertSink[IO, HANAType, CharSequence]].pure[IO]
            case None => IO.raiseError(new RuntimeException("No upsert sink found"))
          }
        }
      rsink map { sink => new Consumer[V] {
        def apply[R <: HList, K <: HList, T <: HList, S <: HList](
            table: String,
            idColumn: Option[Column[HANAType]],
            writeMode: QWriteMode,
            records: Stream[IO, UpsertEvent[R]])(
            implicit
            read: Read[V],
            keys: Keys.Aux[R, K],
            values: Values.Aux[R, V],
            getTypes: Mapper.Aux[columnType.type, V, T],
            rrow: Mapper.Aux[render.type, V, S],
            ktl: ToList[K, String],
            vtl: ToList[S, String],
            ttl: ToList[T, HANAType])
            : IO[(List[V], List[OffsetKey.Actual[String]])] = for {
          columns <- columnsOf(records, idColumn).compile.lastOrError
          colList = (idColumn.get :: columns).map((x: Column[HANAType]) =>
            HANAHygiene.hygienicIdent(Ident(x.name)).asSql).intercalate(fr",")
          dst = ResourcePath.root() / ResourceName(table)
          offsets <- toUpsertSink(sink, dst, idColumn.get, writeMode, records).compile.toList
          q = fr"SELECT" ++
            colList ++
            fr"FROM" ++
            HANAHygiene.hygienicIdent(Ident(table)).asSql
          rows <- runDb[List[V]](q.query[V].to[List])
        } yield (rows, offsets)
      }}
    }

    def append[V <: HList](cfg: Json = config()): Resource[IO, Consumer[V]] = {
      val rsink: Resource[IO, ResultSink.AppendSink[IO, HANAType]] =
        dest(cfg) evalMap { dst =>
          val optSink = dst.sinks.toList.collectFirst { case c @ ResultSink.AppendSink(_) => c }
          optSink match {
            case Some(s) => s.asInstanceOf[ResultSink.AppendSink[IO, HANAType]].pure[IO]
            case None => IO.raiseError(new RuntimeException("No append sink found"))
          }
        }

      rsink map { sink => new Consumer[V] {
        def apply[R <: HList, K <: HList, T <: HList, S <: HList](
            table: String,
            idColumn: Option[Column[HANAType]],
            writeMode: QWriteMode,
            records: Stream[IO, UpsertEvent[R]])(
            implicit
            read: Read[V],
            keys: Keys.Aux[R, K],
            values: Values.Aux[R, V],
            getTypes: Mapper.Aux[columnType.type, V, T],
            rrow: Mapper.Aux[render.type, V, S],
            ktl: ToList[K, String],
            vtl: ToList[S, String],
            ttl: ToList[T, HANAType])
            : IO[(List[V], List[OffsetKey.Actual[String]])] = for {
          tableColumns <- columnsOf(records, idColumn).compile.lastOrError
          colList = (idColumn.toList ++ tableColumns).map((x: Column[HANAType]) =>
            HANAHygiene.hygienicIdent(Ident(x.name)).asSql).intercalate(fr",")
          dst = ResourcePath.root() / ResourceName(table)
          offsets <- toAppendSink(sink, dst, idColumn, writeMode, records).compile.toList
          q = fr"SELECT" ++
            colList ++
            fr"FROM" ++
            HANAHygiene.hygienicIdent(Ident(table)).asSql
          rows <- runDb[List[V]](q.query[V].to[List])
        } yield (rows, offsets)
      }}
    }

    type MkOption = Column[HANAType] => Option[Column[HANAType]]

    object appendAndUpsert {
      def apply[A <: HList]: PartiallyApplied[A] = new PartiallyApplied[A]

      final class PartiallyApplied[A <: HList] {
        def apply[R: AsResult](f: (MkOption, Consumer[A]) => IO[R]): SFragment = {
          "upsert" >> Consumer.upsert[A]().use(f(Some(_), _))
          "append" >> Consumer.append[A]().use(f(Some(_), _))
          "no-id" >> Consumer.append[A]().use(f(x => None, _))
        }
      }
    }

    object idOnly {
      def apply[A <: HList]: PartiallyApplied[A] = new PartiallyApplied[A]

      final class PartiallyApplied[A <: HList] {
        def apply[R: AsResult](f: (MkOption, Consumer[A]) => IO[R]): SFragment = {
          "upsert" >> Consumer.upsert[A]().use(f(Some(_), _))
          "append" >> Consumer.append[A]().use(f(Some(_), _))
        }
      }
    }
  }

  def runDb[A](fa: ConnectionIO[A]): IO[A] =
    TestXa(TestUrl).use(xa => fa.transact(xa))

  def columnsOf[F[_]: Async, R <: HList, K <: HList, V <: HList, T <: HList](
      events: Stream[F, UpsertEvent[R]],
      idColumn: Option[Column[HANAType]])(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      getTypes: Mapper.Aux[columnType.type, V, T],
      ktl: ToList[K, String],
      ttl: ToList[T, HANAType])
      : Stream[F, List[Column[HANAType]]] = {
    def go(inp: Stream[F, UpsertEvent[R]]): Pull[F, List[Column[HANAType]], Unit] = inp.pull.uncons1 flatMap {
      case Some((UpsertEvent.Create(records), tail)) => records.headOption match {
        case Some(r) =>
          val rkeys = r.keys.toList
          val rtypes = r.values.map(columnType).toList
          val columns = rkeys.zip(rtypes).map((Column[HANAType] _).tupled)
          Pull.output1(columns.filter(c => c.some =!= idColumn)) >> Pull.done
        case None =>
          Pull.done
      }
      case Some((_, tail)) =>
        go(tail)
      case _ =>
        Pull.done

    }
    go(events).stream
  }

  def toAppendSink[F[_]: Async, R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
      sink: ResultSink.AppendSink[F, HANAType],
      dst: ResourcePath,
      idColumn: Option[Column[HANAType]],
      writeMode: QWriteMode,
      events: Stream[F, UpsertEvent[R]])(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      getTypes: Mapper.Aux[columnType.type, V, T],
      renderValues: Mapper.Aux[render.type, V, S],
      ktl: ToList[K, String],
      stl: ToList[S, String],
      ttl: ToList[T, HANAType])
      : Stream[F, OffsetKey.Actual[String]] = {

    val appends = appendEvents[F, R, V, S](events)

    type Consumed = ResultSink.AppendSink.Result[F] { type A = CharSequence }
    def toConsumed(a: ResultSink.AppendSink.Result[F]): Consumed = a.asInstanceOf[Consumed]

    for {
      colList <- columnsOf(events, idColumn)
      cols = idColumn match {
        case None => PushColumns.NoPrimary(NonEmptyList.fromListUnsafe(colList))
        case Some(i) => PushColumns.HasPrimary(List(), i, colList)
      }
      consumed = sink.consume.apply(
        ResultSink.AppendSink.Args(dst, cols, writeMode))
      res <- appends.through(toConsumed(consumed).pipe[String])
    } yield res
  }

  def appendEvents[F[_]: Async, R <: HList, V <: HList, S <: HList](
      events: Stream[F, UpsertEvent[R]])(
      implicit
      values: Values.Aux[R, V],
      renderValues: Mapper.Aux[render.type, V, S],
      stl: ToList[S, String])
      : Stream[F, AppendEvent[CharSequence, OffsetKey.Actual[String]]] = events flatMap {
    case UpsertEvent.Commit(s) =>
      Stream(DataEvent.Commit(OffsetKey.Actual.string(s)))
    case UpsertEvent.Delete(_) =>
      Stream.eval_(Async[F].raiseError(new RuntimeException("AppendSink can't handle delete events")))
    case UpsertEvent.Create(records) =>
      Stream.emits(records)
        .covary[F]
        .map(r => Chunk(r.values.map(render).toList.mkString(",")))
        .map(DataEvent.Create(_))
  }

  def toUpsertSink[F[_]: Async, R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
      sink: ResultSink.UpsertSink[F, HANAType, CharSequence],
      dst: ResourcePath,
      idColumn: Column[HANAType],
      writeMode: QWriteMode,
      events: Stream[F, UpsertEvent[R]])(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      getTypes: Mapper.Aux[columnType.type, V, T],
      renderValues: Mapper.Aux[render.type, V, S],
      ktl: ToList[K, String],
      stl: ToList[S, String],
      ttl: ToList[T, HANAType])
      : Stream[F, OffsetKey.Actual[String]] = {
    val upserts = dataEvents[F, R, V, S](events)

    columnsOf(events, Some(idColumn)) flatMap { cols =>
      val (_, pipe) = sink.consume.apply(
        ResultSink.UpsertSink.Args(dst, idColumn, cols, writeMode))
      upserts.through(pipe[String])
    }
  }

  def dataEvents[F[_]: Async, R <: HList, V <: HList, S <: HList](
      events: Stream[F, UpsertEvent[R]])(
      implicit
      values: Values.Aux[R, V],
      renderValues: Mapper.Aux[render.type, V, S],
      stl: ToList[S, String])
      : Stream[F, DataEvent[CharSequence, OffsetKey.Actual[String]]] = events flatMap {
    case UpsertEvent.Commit(s) =>
      Stream(DataEvent.Commit(OffsetKey.Actual.string(s)))
    case UpsertEvent.Delete(ids) => ids match {
      case Ids.StringIds(is) =>
        Stream(DataEvent.Delete(IdBatch.Strings(is.toArray, is.length)))
      case Ids.LongIds(is) =>
        Stream(DataEvent.Delete(IdBatch.Longs(is.toArray, is.length)))
    }
    case UpsertEvent.Create(records) =>
      Stream.emits(records).covary[F] map { r =>
        DataEvent.Create(Chunk(r.values.map(render).toList.mkString(",")))
      }
  }

  sealed trait UpsertEvent[+A] extends Product with Serializable
  sealed trait Ids extends Product with Serializable

  object UpsertEvent {
    case class Create[A](records: List[A]) extends UpsertEvent[A]
    case class Delete(recordIds: Ids) extends UpsertEvent[Nothing]
    case class Commit(value: String) extends UpsertEvent[Nothing]
  }

  object Ids {
    case class StringIds(ids: List[String]) extends Ids
    case class LongIds(ids: List[Long]) extends Ids
  }

  object columnType extends Poly1 {
    import HANAType._
    // Set to the 'preferred/default' coercion for 'ColumnType.String'
    // to test indexability adjustments are applied.
    implicit val stringCase: Case.Aux[String, HANAType] = at(_ => VARCHAR(512))
    implicit val intCase: Case.Aux[Int, HANAType] = at(_ => INTEGER)
  }

  object render extends Poly1 {
    implicit val stringCase: Case.Aux[String, String] = at(s => s"'$s'")
    implicit val intCase: Case.Aux[Int, String] = at(_.toString)
  }

  // This is needed only for testing purpose
  implicit val eqHANAType: Eq[HANAType] = Eq.fromUniversalEquals
}


