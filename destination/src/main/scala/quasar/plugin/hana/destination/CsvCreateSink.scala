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

import quasar.plugin.hana._

import scala._, Predef._

import quasar.api.Column
import quasar.api.resource.ResourcePath
import quasar.connector.MonadResourceErr
import quasar.connector.render.RenderConfig
import quasar.lib.jdbc._
import quasar.lib.jdbc.destination.WriteMode

import java.lang.CharSequence

import cats.data.NonEmptyList
import cats.effect.ConcurrentEffect
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.{Pipe, Stream}

import org.slf4s.Logger

import shims._

private[destination] object CsvCreateSink {
  def apply[F[_]: MonadResourceErr: ConcurrentEffect](
      writeMode: WriteMode,
      xa: Transactor[F],
      logger: Logger)(
      path: ResourcePath,
      columns: NonEmptyList[Column[HANAType]])
      : (RenderConfig[CharSequence], Pipe[F, CharSequence, Unit]) = {

    val logHandler = Slf4sLogHandler(logger)

    val renderConfig = RenderConfig.Separated(",", HANAColumnRender(columns))

    val hygienicColumns: NonEmptyList[(HI, HANAType)] =
      columns.map(c => (HANAHygiene.hygienicIdent(Ident(c.name)), c.tpe))

    val objFragmentF: F[(Fragment, String)] =
      MonadResourceErr.unattempt_(pathFragment(path).asScalaz)

    def doLoad(obj: Fragment, unsafeName: String): Pipe[F, CharSequence, Unit] = in => {
      val writeTable: ConnectionIO[Unit] =
        startLoad(logHandler)(writeMode, obj, unsafeName, columns, None)

      def insert(prefix: StringBuilder, length: Int): Stream[F, Unit] =
        Stream.eval(writeTable.void.transact(xa)) ++
          in.chunks.evalMap(insertChunk(logHandler)(obj, hygienicColumns, _).transact(xa))

      val (prefix, length) = insertIntoPrefix(logHandler)(obj, hygienicColumns)

      insert(prefix, length)
    }

    (renderConfig, in => Stream.eval(objFragmentF) flatMap {
      case (obj, str) => doLoad(obj, str)(in)
    })
  }
}
