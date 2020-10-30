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


import scala._, Predef._

import quasar.api.push.OffsetKey
import quasar.connector.destination.ResultSink.UpsertSink
import quasar.connector.render.RenderConfig
import quasar.connector.{DataEvent, IdBatch, MonadResourceErr}
import quasar.plugin.jdbc._
import quasar.plugin.jdbc.destination.{WriteMode => JWriteMode}

import java.lang.CharSequence

import cats.data.NonEmptyVector
import cats.effect.{Effect, Timer}
import cats.implicits._

import doobie._
import doobie.free.connection.{rollback, setAutoCommit, unit}
import doobie.implicits._
import doobie.util.transactor.Strategy

import fs2.{Chunk, Pipe, Stream}

import org.slf4s.Logger

import skolems.∀

import shims._

private[destination] object CsvUpsertSink {
  type Consume[F[_], A] =
    Pipe[F, DataEvent[CharSequence, OffsetKey.Actual[A]], OffsetKey.Actual[A]]

  def apply[F[_]: Effect: MonadResourceErr](
    writeMode: JWriteMode,
    xa0: Transactor[F],
    logger: Logger)(implicit timer: Timer[F])
      : UpsertSink.Args[HANAType] => (RenderConfig[CharSequence], ∀[Consume[F, ?]]) = {

    val strategy = Strategy(setAutoCommit(false), unit, rollback, unit)
    val xa = Transactor.strategy.modify(xa0, _ => strategy)

    run(xa, writeMode, logger)
  }

  private def run[F[_]: Effect: MonadResourceErr](
    xa: Transactor[F],
    writeMode: JWriteMode,
    logger: Logger)(
    args: UpsertSink.Args[HANAType])(implicit timer: Timer[F])
      : (RenderConfig[CharSequence], ∀[Consume[F, ?]]) = {

    val logHandler = Slf4sLogHandler(logger)

    val columns = hygienicColumns(args.columns)

    def load[A](dataEvents: Stream[F, DataEvent[CharSequence, OffsetKey.Actual[A]]])
        : Stream[F, OffsetKey.Actual[A]] = {

      def deleteBatch(recordIds: IdBatch, objFragment: Fragment): Fragment = {
        val preamble: Fragment =
          fr"DELETE FROM" ++
            objFragment ++
            fr"WHERE" ++
            Fragment.const(args.idColumn.name)

        recordIds match {
          case IdBatch.Strings(values, size) =>
            Fragments.in(preamble, NonEmptyVector.fromVectorUnsafe(values.take(size).toVector))
          case IdBatch.Longs(values, size) =>
            Fragments.in(preamble, NonEmptyVector.fromVectorUnsafe(values.take(size).toVector))
          case IdBatch.Doubles(values, size) =>
            Fragments.in(preamble, NonEmptyVector.fromVectorUnsafe(values.take(size).toVector))
          case IdBatch.BigDecimals(values, size) =>
            Fragments.in(preamble, NonEmptyVector.fromVectorUnsafe(values.take(size).toVector))
        }
      }

      def logEvents(event: DataEvent[CharSequence, _]): F[Unit] =
        event match {
          case DataEvent.Create(chunk) =>
            trace(logger)(s"Loading chunk with size: ${chunk.size}")

          case DataEvent.Delete(idBatch) =>
            trace(logger)(s"Deleting ${idBatch.size} records")

          case DataEvent.Commit(_) =>
            trace(logger)("Ignoring commit")
        }

      def handleEvents(objFragment: Fragment, unsafeName: String)
          : Pipe[F, DataEvent[CharSequence, OffsetKey.Actual[A]], Option[OffsetKey.Actual[A]]] =
        _ evalMap {
          case DataEvent.Create(records) =>
            insertChunk(logHandler)(objFragment, columns, records)
              .transact(xa)
              .as(none[OffsetKey.Actual[A]])

          case DataEvent.Delete(recordIds) =>
            deleteBatch(recordIds, objFragment)
              .updateWithLogHandler(logHandler)
              .run
              .transact(xa)
              .as(none[OffsetKey.Actual[A]])

          case DataEvent.Commit(offset) =>
            offset.some.pure[F]
        }

      Stream.force(
        for {
          (objFragment, unsafeName) <- MonadResourceErr.unattempt_(pathFragment(args.path).asScalaz)
          start = startLoad(logHandler)(writeMode, objFragment, unsafeName, args.columns).transact(xa)
          logStart = trace(logger)("Starting load")
          logEnd = trace(logger)("Finished load")

          handled =
            dataEvents
              .evalTap(logEvents)
              .through(handleEvents(objFragment, unsafeName))
              .unNone

          out = Stream.eval_(logStart) ++ Stream.eval_(start) ++ handled ++ Stream.eval_(logEnd)
        } yield out)
    }

    (RenderConfig.Separated(",", HANAColumnRender(args.columns)), ∀[Consume[F, ?]](load(_)))
  }

  private def trace[F[_]: Effect](log: Logger)(msg: => String): F[Unit] =
    Effect[F].delay(println(msg))
}
