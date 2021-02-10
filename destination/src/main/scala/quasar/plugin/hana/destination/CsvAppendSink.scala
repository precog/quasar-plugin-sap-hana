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
import quasar.connector.destination.ResultSink.AppendSink
import quasar.connector.render.RenderConfig
import quasar.connector.{AppendEvent, DataEvent, MonadResourceErr}
import quasar.lib.jdbc.destination.{WriteMode => JWriteMode}

import java.lang.CharSequence

import cats.effect.Effect
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.Pipe
import org.slf4s.Logger

import skolems.∀

private[destination] object CsvAppendSink {
  type Consume[F[_], A] =
    Pipe[F, AppendEvent[CharSequence, OffsetKey.Actual[A]], OffsetKey.Actual[A]]

  def builder[F[_]: Effect: MonadResourceErr](
      writeMode: JWriteMode,
      xa: Transactor[F],
      args: AppendSink.Args[HANAType],
      logger: Logger)
      : CsvSinkBuilder[F, AppendEvent[CharSequence, *]] =
    new CsvSinkBuilder[F, AppendEvent[CharSequence, *]](
      xa,
      args.writeMode,
      writeMode,
      args.path,
      args.pushColumns.primary,
      args.columns,
      logger) {

      // val columns is not in CsvSinkBuilder because that way using them seems magical
      val columns = hygienicColumns(args.columns)

      def logEvents(event: AppendEvent[CharSequence, _]): F[Unit] =
        event match {
          case DataEvent.Create(chunk) =>
            trace(logger)(s"Loading chunk with size: ${chunk.size}")
          case DataEvent.Commit(_) =>
            trace(logger)("Ignoring commit")
        }

      def handleEvents[A](objFragment: Fragment, unsafeName: String)
          : Pipe[F, AppendEvent[CharSequence, OffsetKey.Actual[A]], Option[OffsetKey.Actual[A]]] =
        _ evalMap {
          case DataEvent.Create(records) =>
            insertChunk(logHandler)(objFragment, columns, records)
              .transact(xa)
              .as(none[OffsetKey.Actual[A]])

          // See comment in CsvUpsertSink, it's about HANA pruning long-running transactions
          case DataEvent.Commit(offset) =>
            offset.some.pure[F]
        }
  }

  def apply[F[_]: Effect: MonadResourceErr](
      writeMode: JWriteMode,
      xa: Transactor[F],
      logger: Logger)(
      args: AppendSink.Args[HANAType])
      : (RenderConfig[CharSequence], ∀[Consume[F, *]]) = {
    val config = RenderConfig.Separated(",", HANAColumnRender(args.columns))
    (config, builder(writeMode, xa, args, logger).build)
  }
}


