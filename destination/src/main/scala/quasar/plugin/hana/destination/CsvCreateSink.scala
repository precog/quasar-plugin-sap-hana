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
import quasar.api.resource.{/:, ResourcePath}
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

    val objFragmentF: F[(Fragment, String)] = for {
      dbo <- singleResourcePathRef(path) match {
        case Some(ref) => ref.pure[F]
        case _ => MonadResourceErr[F].raiseError(ResourceError.notAResource(path))
      }
      hygienicRef = HANAHygiene.hygienicIdent(dbo)
      fragment = Fragment.const0(hygienicRef.forSql)
      unsafeName = s"""${dbo.asString}""" // this is fine
    } yield (fragment, unsafeName)

    def ifExists(unsafeName: String): Query0[Int] = {
      (fr0"SELECT count(*) as exists_flag FROM TABLES WHERE TABLE_NAME='" ++ Fragment.const0(unsafeName) ++ fr0"'")
        .queryWithLogHandler[Int](logHandler)
    }

    def replaceTable(objFragment: Fragment, unsafeName: String): ConnectionIO[Int] =
      ifExists(unsafeName).option flatMap { result =>
        if (result.exists(_ == 1)) {
          val drop = (fr"DROP TABLE" ++ objFragment)
            .updateWithLogHandler(logHandler)
            .run
          drop >> createTable(objFragment)
        } else {
          createTable(objFragment)
        }
      }

    def truncateTable(objFragment: Fragment, unsafeName: String): ConnectionIO[Int] =
      ifExists(unsafeName).option flatMap { result =>
        if (result.exists(_ == 1))
          (fr"TRUNCATE TABLE" ++ objFragment)
            .updateWithLogHandler(logHandler)
            .run
        else
          createTable(objFragment)
      }

    def createTable(objFragment: Fragment): ConnectionIO[Int] =
      (fr"CREATE TABLE" ++ objFragment ++ fr0" " ++ createColumnSpecs(hygienicColumns))
        .updateWithLogHandler(logHandler)
        .run

    def insertInto(objFragment: Fragment, value: CharSequence): String =
      (fr"INSERT INTO " ++ objFragment ++ insertColumnSpecs(hygienicColumns) ++ fr" VALUES (" ++
        Fragment.const0(value.toString) ++
        fr")").update.sql

    // TODO support appending
    def doLoad(obj: Fragment, unsafeName: String): Pipe[F, CharSequence, Unit] = in => {
      val writeTable: ConnectionIO[Int] = writeMode match {
        case WriteMode.Create => createTable(obj)
        case WriteMode.Replace => replaceTable(obj, unsafeName)
        case WriteMode.Truncate => truncateTable(obj, unsafeName)
      }

      def connect(statement: String): ConnectionIO[Unit] =
        HC.createStatement(FS.execute(statement).map(_ => ()))

      // TODO can we only transact once per push
      val write: Stream[F, Int] = Stream.eval(writeTable.transact(xa))

      val insert: Stream[F, Unit] = in evalMap { chars =>
        connect(insertInto(obj, chars)).transact(xa)
      }

      write.drain ++ insert
    }

    (renderConfig, in => Stream.eval(objFragmentF) flatMap {
      case (obj, str) => doLoad(obj, str)(in)
    })
  }

  private def createColumnSpecs(cols: NonEmptyList[(HI, HANAType)]): Fragment =
    Fragments.parentheses(
      cols
        .map { case (n, t) => Fragment.const(n.forSql) ++ t.asSql }
        .intercalate(fr","))

  private def insertColumnSpecs(cols: NonEmptyList[(HI, HANAType)]): Fragment =
    Fragments.parentheses(
      cols
        .map { case (n, _) => Fragment.const(n.forSql) }
        .intercalate(fr","))

   def singleResourcePathRef(p: ResourcePath): Option[Ident] =
    Some(p) collect {
      case fst /: ResourcePath.Root => Ident(fst)
    }
}
