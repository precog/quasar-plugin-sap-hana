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
    TypeCoercion.Satisfied(NonEmptyList.one(HANAType.BOOLEAN))
  }

  def construct(id: TypeId): Either[Type, Constructor[Type]] = {
    Left(HANAType.BOOLEAN)
  }
}

object HANADestination {
  val OrdinalMap: Map[Int, HANATypeId] =
    HANATypeId.allIds
      .toList
      .map(id => (id.ordinal, id))
      .toMap
}
