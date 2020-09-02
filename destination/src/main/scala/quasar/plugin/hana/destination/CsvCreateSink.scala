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
import quasar.plugin.jdbc.implicits._
import quasar.plugin.jdbc.destination.WriteMode

import java.lang.CharSequence

import cats.data.NonEmptyList
import cats.effect.ConcurrentEffect
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.{Chunk, Pipe, Stream}

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

    def appendToTable(objFragment: Fragment, unsafeName: String): ConnectionIO[Int] =
      ifExists(unsafeName).option flatMap { result =>
        if (result.exists(_ == 1))
          0.pure[ConnectionIO]
        else
          createTable(objFragment)
      }

    def createTable(objFragment: Fragment): ConnectionIO[Int] =
      (fr"CREATE TABLE" ++ objFragment ++ fr0" " ++ createColumnSpecs(hygienicColumns))
        .updateWithLogHandler(logHandler)
        .run

    def insertIntoPrefix(objFragment: Fragment): (StringBuilder, Int) = {
      val value = (
        fr"INSERT INTO" ++
          objFragment ++
          insertColumnSpecs(hygienicColumns) ++
          fr0" VALUES (").update.sql

      val builder = new StringBuilder(value)

      (builder, builder.length)
    }

    def insertInto(prefix: StringBuilder, value: CharSequence): StringBuilder =
      prefix.append(value).append(')')

    def doLoad(obj: Fragment, unsafeName: String): Pipe[F, CharSequence, Unit] = in => {
      val writeTable: ConnectionIO[Int] = writeMode match {
        case WriteMode.Create => createTable(obj)
        case WriteMode.Replace => replaceTable(obj, unsafeName)
        case WriteMode.Truncate => truncateTable(obj, unsafeName)
        case WriteMode.Append => appendToTable(obj, unsafeName)
      }

      def insertBatch(prefix: StringBuilder, length: Int, chunk: Chunk[CharSequence])
          : ConnectionIO[Unit] = {
        val batch = FS.raw { statement =>
          chunk foreach { value =>
            val sql = insertInto(prefix, value)
            statement.addBatch(sql.toString)
            prefix.setLength(length)
          }

          statement.executeBatch()
        }

        HC.createStatement(batch).map(_ => ())
      }

      def insert(prefix: StringBuilder, length: Int): Stream[F, Unit] =
        Stream.resource(xa.strategicConnection) flatMap { c =>
          Stream.eval(xa.runWith(c).apply(writeTable.map(_ => ()))) ++
            in.chunks.evalMap(chunk => xa.runWith(c).apply(insertBatch(prefix, length, chunk)))
        }

      val (prefix, length) = insertIntoPrefix(obj)

      insert(prefix, length)
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
