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

package quasar.plugin.hana

import scala._, Predef._

import quasar.api.Column
import quasar.api.resource.{/:, ResourcePath}
import quasar.connector.ResourceError
import quasar.plugin.jdbc.Ident
import quasar.plugin.jdbc.destination.WriteMode

import cats.data.NonEmptyList
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2._

import java.lang.CharSequence

package object destination {
  def ifExists(logHandler: LogHandler)(unsafeName: String): Query0[Int] = {
    (fr0"SELECT count(*) as exists_flag FROM TABLES WHERE TABLE_NAME='" ++ Fragment.const0(unsafeName) ++ fr0"'")
      .queryWithLogHandler[Int](logHandler)
  }

  def replaceTable(logHandler: LogHandler)(
    objFragment: Fragment,
    unsafeName: String,
    columns: NonEmptyList[Column[HANAType]])
      : ConnectionIO[Int] =
    ifExists(logHandler)(unsafeName).option flatMap { result =>
      if (result.exists(_ == 1)) {
        val drop = (fr"DROP TABLE" ++ objFragment)
          .updateWithLogHandler(logHandler)
          .run
        drop >> createTable(logHandler)(objFragment, columns)
      } else {
        createTable(logHandler)(objFragment, columns)
      }
    }

  def truncateTable(logHandler: LogHandler)(
    objFragment: Fragment,
    unsafeName: String,
    columns: NonEmptyList[Column[HANAType]])
      : ConnectionIO[Int] =
    ifExists(logHandler)(unsafeName).option flatMap { result =>
      if (result.exists(_ == 1))
        (fr"TRUNCATE TABLE" ++ objFragment)
          .updateWithLogHandler(logHandler)
          .run
      else
        createTable(logHandler)(objFragment, columns)
    }

  def appendToTable(logHandler: LogHandler)(
    objFragment: Fragment,
    unsafeName: String,
    columns: NonEmptyList[Column[HANAType]])
      : ConnectionIO[Int] =
    ifExists(logHandler)(unsafeName).option flatMap { result =>
      if (result.exists(_ == 1))
        0.pure[ConnectionIO]
      else
        createTable(logHandler)(objFragment, columns)
    }

  def createTable(logHandler: LogHandler)(objFragment: Fragment, columns: NonEmptyList[Column[HANAType]])
      : ConnectionIO[Int] =
    (fr"CREATE TABLE" ++ objFragment ++ fr0" " ++ createColumnSpecs(hygienicColumns(columns)))
      .updateWithLogHandler(logHandler)
      .run

  def hygienicColumns(columns: NonEmptyList[Column[HANAType]]): NonEmptyList[(HI, HANAType)] =
    columns.map(c => (HANAHygiene.hygienicIdent(Ident(c.name)), c.tpe))

  def createColumnSpecs(cols: NonEmptyList[(HI, HANAType)]): Fragment =
    Fragments.parentheses(
      cols
        .map { case (n, t) => Fragment.const(n.forSql) ++ t.asSql }
        .intercalate(fr","))

  def insertColumnSpecs(cols: NonEmptyList[(HI, HANAType)]): Fragment =
    Fragments.parentheses(
      cols
        .map { case (n, _) => Fragment.const(n.forSql) }
        .intercalate(fr","))

  def insertIntoPrefix(objFragment: Fragment, cols: NonEmptyList[(HI, HANAType)])
      : (StringBuilder, Int) = {
    val value = (
      fr"INSERT INTO" ++
        objFragment ++
        insertColumnSpecs(cols) ++
        fr0" VALUES (").update.sql

    val builder = new StringBuilder(value)

    (builder, builder.length)
  }

  def insertChunk(
    objFragment: Fragment,
    cols: NonEmptyList[(HI, HANAType)],
    chunk: Chunk[CharSequence])
      : ConnectionIO[Unit] = {
    val (prefix, length) = insertIntoPrefix(objFragment, cols)
    val batch = FS.raw { statement =>
      chunk foreach { value =>
        val sql = prefix.append(value).append(')')

        statement.addBatch(sql.toString)
        prefix.setLength(length)
      }

      statement.executeBatch()
    }

    HC.createStatement(batch).void
  }

  def pathFragment(path: ResourcePath): Either[ResourceError, (Fragment, String)] =
    for {
      dbo <- singleResourcePathRef(path).toRight(ResourceError.notAResource(path))
      hygienicRef = HANAHygiene.hygienicIdent(dbo)
      fragment = Fragment.const0(hygienicRef.forSql)
      unsafeName = s"""${dbo.asString}""" // this is fine
    } yield (fragment, unsafeName)

  def singleResourcePathRef(p: ResourcePath): Option[Ident] =
    Some(p) collect {
      case fst /: ResourcePath.Root => Ident(fst)
    }

  def startLoad(logHandler: LogHandler)(
    writeMode: WriteMode,
    obj: Fragment,
    unsafeName: String,
    columns: NonEmptyList[Column[HANAType]])
      : ConnectionIO[Int] = writeMode match {
    case WriteMode.Create => createTable(logHandler)(obj, columns)
    case WriteMode.Replace => replaceTable(logHandler)(obj, unsafeName, columns)
    case WriteMode.Truncate => truncateTable(logHandler)(obj, unsafeName, columns)
    case WriteMode.Append => appendToTable(logHandler)(obj, unsafeName, columns)
  }
}
