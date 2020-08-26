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

import quasar.api.Column
import quasar.api.resource.ResourcePath
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.connector.render.RenderConfig
import quasar.plugin.jdbc._
import quasar.plugin.jdbc.destination.WriteMode

import java.lang.CharSequence

import cats.data.NonEmptyList
import cats.effect.ConcurrentEffect
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.{Pipe, Stream}

import org.slf4s.Logger

object CsvCreateSink {
  def apply[F[_]: MonadResourceErr: ConcurrentEffect](
      writeMode: WriteMode,
      xa: Transactor[F],
      logger: Logger)(
      path: ResourcePath,
      cols: NonEmptyList[Column[HANAType]])
      : (RenderConfig[CharSequence], Pipe[F, CharSequence, Unit]) = {

    val renderConfig = RenderConfig.Separated(",", HANAColumnRender)

    // make table with columns provided
    // respect write mode
    //

    def load(chars: Stream[F, CharSequence]): Stream[F, Unit] = {
      resourcePathRef(path) match {
        case Some(ref) => ref.pure[F]
        case _ => MonadResourceErr[F].raiseError(ResourceError.notAResource(path))
      }

      //def insertStatement(value: CharSequence) =
      //  fr"INSERT INTO QuinnCat (col1) VALUES (16)".update.sql

      def insertStatement(value: CharSequence): String =
        (fr"INSERT INTO QuinnCat (col1) VALUES (" ++
          Fragment.const0(value.toString) ++
          fr")").update.sql

      def connect(value: CharSequence): ConnectionIO[Unit] =
        HC.createStatement(FS.execute(insertStatement(value)).map(_ => ()))

      chars.evalMap(v => {
        logger.info(s">>>> v: $v")
        connect(v).transact(xa)
      })
    }

    (renderConfig, load(_))
  }

  private def columnSpecs(cols: NonEmptyList[Column[HANAType]]): Fragment =
    Fragments.parentheses(
      cols
        .map(_.tpe.asSql)
        .intercalate(fr","))
}
