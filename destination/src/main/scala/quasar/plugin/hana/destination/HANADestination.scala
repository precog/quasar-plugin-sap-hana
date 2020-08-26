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

import quasar.api.{ColumnType, Label}
import quasar.api.push.TypeCoercion
import quasar.connector.MonadResourceErr
import quasar.connector.destination.{Constructor, Destination, ResultSink}
import quasar.plugin.jdbc.destination.WriteMode

import cats.data.NonEmptyList
import cats.effect.{ConcurrentEffect, Timer}

import doobie.Transactor

import monocle.Prism

import org.slf4s.Logger

private[destination] final class HANADestination[F[_]: ConcurrentEffect: MonadResourceErr: Timer](
    writeMode: WriteMode,
    xa: Transactor[F],
    logger: Logger)
    extends Destination[F] {

  type Type = HANAType
  type TypeId = HANATypeId

  val destinationType = HANADestinationModule.destinationType

  val typeIdOrdinal: Prism[Int, TypeId] =
    Prism(HANADestination.OrdinalMap.get(_))(_.ordinal)

  val typeIdLabel: Label[TypeId] =
    Label.label[TypeId](_.toString)

  val sink: ResultSink[F, Type] =
    ResultSink.CreateSink(CsvCreateSink[F](writeMode, xa, logger))

  val sinks: NonEmptyList[ResultSink[F, Type]] = NonEmptyList.one(sink)

  def coerce(tpe: ColumnType.Scalar): TypeCoercion[TypeId] = {
    def satisfied(t: TypeId, ts: TypeId*) =
      TypeCoercion.Satisfied(NonEmptyList(t, ts.toList))

    tpe match {
      case ColumnType.Boolean => satisfied(
        HANAType.BOOLEAN)

      case ColumnType.Number => satisfied(
        HANAType.DOUBLE,
        HANAType.INTEGER,
        HANAType.DECIMAL,
        HANAType.SMALLDECIMAL,
        HANAType.BIGINT,
        HANAType.SMALLINT,
        HANAType.TINYINT,
        HANAType.REAL,
        HANAType.FLOAT)

      case ColumnType.String => satisfied(
        HANAType.NVARCHAR,
        HANAType.VARCHAR,
        HANAType.TEXT,
        HANAType.SHORTTEXT,
        HANAType.ALPHANUM,
        HANAType.CLOB,
        HANAType.NCLOB,
        HANAType.VARBINARY,
        HANAType.BLOB)

      case ColumnType.LocalDate => satisfied(
        HANAType.DATE)

      case ColumnType.LocalTime => satisfied(
        HANAType.TIME)

      case ColumnType.LocalDateTime => satisfied(
        HANAType.TIMESTAMP,
        HANAType.SECONDDATE)

      case ColumnType.OffsetDate =>
        TypeCoercion.Unsatisfied(List(ColumnType.LocalDate), None)

      case ColumnType.OffsetTime =>
        TypeCoercion.Unsatisfied(List(ColumnType.LocalTime), None)

      case ColumnType.OffsetDateTime =>
        TypeCoercion.Unsatisfied(List(ColumnType.LocalDateTime), None)

      case ColumnType.Interval =>
        TypeCoercion.Unsatisfied(Nil, None)

      case ColumnType.Null =>
        TypeCoercion.Unsatisfied(Nil, None)
    }
  }

  def construct(id: TypeId): Either[Type, Constructor[Type]] = {
    id match {
      case tpe: HANATypeId.SelfIdentified => Left(tpe)
      case hk: HANATypeId.HigherKinded => Right(hk.constructor)
    }
  }
}

object HANADestination {
  val OrdinalMap: Map[Int, HANATypeId] =
    HANATypeId.allIds
      .toList
      .map(id => (id.ordinal, id))
      .toMap
}
