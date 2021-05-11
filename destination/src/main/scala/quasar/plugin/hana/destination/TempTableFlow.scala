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

import quasar.api.Column
import quasar.connector.{MonadResourceErr, ResourceError, IdBatch}
import quasar.connector.destination.{WriteMode => QWriteMode}
import quasar.lib.jdbc.{Ident, Slf4sLogHandler}
import quasar.lib.jdbc.destination.WriteMode
import quasar.lib.jdbc.destination.flow.{Flow, FlowArgs}
import quasar.plugin.hana._

import cats.data.NonEmptyList
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.free.connection.commit

import fs2.Chunk

import java.lang.CharSequence

import org.slf4s.Logger

object TempTableFlow {
  def apply[F[_]: Sync: MonadResourceErr](
      xa: Transactor[F],
      logger: Logger,
      writeMode: WriteMode,
      args: FlowArgs[HANAType])
      : Resource[F, Flow[CharSequence]] = {

    val log = Slf4sLogHandler(logger)

    def checkWriteMode(unsafeName: String): F[Unit] = {
      val existing: ConnectionIO[Boolean] = ifExists(log)(unsafeName).option map { results =>
        results.exists(_ === 1)
      }
      writeMode match {
        case WriteMode.Create => existing.transact(xa) flatMap { exists =>
          MonadResourceErr[F].raiseError(
            ResourceError.accessDenied(
              args.path,
              "Create mode is set but the table exists already".some,
              none)).whenA(exists)
        }
        case _ =>
          ().pure[F]
      }
    }

    val acquire: F[(TempTable, Flow[CharSequence])] = for {
      (fragment, unsafeName) <- pathFragment(args.path) match {
        case Left(e) => MonadResourceErr[F].raiseError(e)
        case Right(a) => a.pure[F]
      }
      _ <- checkWriteMode(unsafeName)
      tempTable = TempTable(
        log,
        writeMode,
        unsafeName,
        fragment,
        args.columns,
        args.idColumn,
        args.filterColumn)
      _ <- {
        tempTable.drop >>
        tempTable.create >>
        commit
      }.transact(xa)
      refMode <- Ref.in[F, ConnectionIO, QWriteMode](args.writeMode)
    } yield {
      val flow = new Flow[CharSequence] {
        def delete(ids: IdBatch): ConnectionIO[Unit] =
          ().pure[ConnectionIO]

        def ingest(chunk: Chunk[CharSequence]): ConnectionIO[Unit] =
          tempTable.ingest(chunk) >> commit

        def replace = refMode.get flatMap {
          case QWriteMode.Replace =>
            tempTable.persist >> commit >> refMode.set(QWriteMode.Append)
          case QWriteMode.Append =>
            append
        }

        def append =
          tempTable.append >> commit
      }

      (tempTable, flow)
    }

    val release: ((TempTable, _)) => F[Unit] = { case (tempTable, _) =>
      (tempTable.drop >> commit).transact(xa)
    }

    Resource.make(acquire)(release).map(_._2)
  }

  private trait TempTable {
    def ingest(chunk: Chunk[CharSequence]): ConnectionIO[Unit]
    def drop: ConnectionIO[Unit]
    def create: ConnectionIO[Unit]
    def persist: ConnectionIO[Unit]
    def append: ConnectionIO[Unit]
  }

  private object TempTable {
    def apply(
        log: LogHandler,
        writeMode: WriteMode,
        unsafeName: String,
        tgtFragment: Fragment,
        columns: NonEmptyList[Column[HANAType]],
        idColumn: Option[Column[_]],
        filterColumn: Option[Column[_]])
        : TempTable = {
      val tempName = s"precog_temp_$unsafeName"
      val tmpFragment = Fragment.const0(HANAHygiene.hygienicIdent(Ident(unsafeName)).forSql)

      val hyColumns = hygienicColumns(columns)

      val run: Fragment => ConnectionIO[Unit] =
        _.updateWithLogHandler(log).run.void

      def whenExists(s: String): Fragment => ConnectionIO[Unit] = fr =>
        ifExists(log)(s).option.flatMap { results =>
          run(fr).whenA(results.exists(_ === 1))
        }

      def unlessExists(s: String): Fragment => ConnectionIO[Unit] = fr =>
        ifExists(log)(s).option.flatMap { results =>
          run(fr).unlessA(results.exists(_ === 1))
        }


      def truncate: ConnectionIO[Unit] = whenExists(tempName) {
        fr"TRUNCATE TABLE" ++ tmpFragment
      }

      def insertInto: ConnectionIO[Unit] = run {
        fr"INSERT INTO" ++ tgtFragment ++ fr0" " ++
        fr"SELECT * FROM" ++ tmpFragment
      }

      def rename: ConnectionIO[Unit] = run {
        fr"RENAME TABLE" ++ tmpFragment ++ fr" TO" ++ tgtFragment
      }

      def filter(idColumn: Column[_]): ConnectionIO[Unit] = run {
        val mkColumn: String => Fragment = parent =>
          Fragment.const0(parent) ++ fr0"." ++
          Fragment.const0(HANAHygiene.hygienicIdent(Ident(idColumn.name)).forSql)
        // SAP Hana doesn't support DELETE INNER JOIN, but supports MERGE 0_o
        fr"MERGE INTO" ++ tgtFragment ++ fr" AS TGT" ++
        fr"USING" ++ tmpFragment ++ fr" AS TMP" ++
        fr"ON" ++ mkColumn("TGT") ++ fr0"=" ++ mkColumn("TMP")
        fr"WHEN MATCHED THEN DELETE"
      }

      def merge: ConnectionIO[Unit] =
        filterColumn.traverse_(filter(_)) >>
        insertInto

      def createTgt: ConnectionIO[Unit] = run {
        fr"CREATE TABLE" ++ tmpFragment ++ fr0" " ++ createColumnSpecs(hyColumns)
      }

      def dropTgtIfExists: ConnectionIO[Unit] = whenExists(unsafeName) {
        fr"DROP TABLE" ++ tgtFragment
      }

      def truncateTgt: ConnectionIO[Unit] = run {
        fr"TRUNCATE TABLE" ++ tgtFragment
      }

      def createTgtIfNotExists: ConnectionIO[Unit] = unlessExists(unsafeName) {
        fr"CREATE TABLE" ++ tmpFragment ++ fr0" " ++ createColumnSpecs(hyColumns)
      }

      def index(tbl: Fragment, col: Column[_]): ConnectionIO[Unit] =
        ifIndexExists(log)(unsafeName).option flatMap { results =>
          val colFragment = Fragments.parentheses(Fragment.const(HANAHygiene.hygienicIdent(Ident(col.name)).forSql))
          createIndex(log)(tbl, unsafeName, colFragment).unlessA(results.exists(_ === 1))
        }

      new TempTable {
        def ingest(chunk: Chunk[CharSequence]): ConnectionIO[Unit] =
          insertChunk(log)(tmpFragment, hyColumns, chunk)

        def drop: ConnectionIO[Unit] = whenExists(tempName) {
          fr"DROP TABLE" ++ tmpFragment
        }

        def create: ConnectionIO[Unit] =
          unlessExists(tempName)(fr"CREATE TABLE" ++ tmpFragment ++ fr0" " ++ createColumnSpecs(hyColumns)) >>
          filterColumn.traverse_(index(tmpFragment, _))

        def append: ConnectionIO[Unit] =
          merge >>
          truncate

        def persist: ConnectionIO[Unit] = {
          val indexColumn =
            if (idColumn.map(_.name) =!= filterColumn.map(_.name))
              idColumn
            else
              none[Column[_]]

          val prepare = writeMode match {
            case WriteMode.Create =>
              createTgt >>
              idColumn.traverse_(index(tgtFragment, _))
              append
            case WriteMode.Replace =>
              dropTgtIfExists >>
              rename >>
              create
            case WriteMode.Truncate =>
              createTgtIfNotExists >>
              truncateTgt >>
              append
            case WriteMode.Append =>
              createTgtIfNotExists >>
              append
          }
          prepare >>
          indexColumn.traverse_(index(tgtFragment, _))
        }
      }
    }
  }
}

