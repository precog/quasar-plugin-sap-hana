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

import java.lang.CharSequence
import java.time._

import quasar.connector.render.ColumnRender

import qdata.time.{DateTimeInterval, OffsetDate}

object HANAColumnRender extends ColumnRender[CharSequence] {

  def renderUndefined(columnName: String): CharSequence = "NULL"

  def renderNull(columnName: String): CharSequence = "NULL"

  def renderEmptyArray(columnName: String): CharSequence = "FIXME"

  def renderEmptyObject(columnName: String): CharSequence = "FIXME"

  def renderBoolean(columnName: String, value: Boolean): CharSequence = "FIXME"

  def renderLong(columnName: String, value: Long): CharSequence = value.toString

  def renderDouble(columnName: String, value: Double): CharSequence = value.toString

  def renderBigDecimal(columnName: String, value: BigDecimal): CharSequence = value.toString

  def renderString(columnName: String, value: String): CharSequence = value

  def renderLocalTime(columnName: String, value: LocalTime): CharSequence = "FIXME"

  def renderOffsetTime(columnName: String, value: OffsetTime): CharSequence = "FIXME"

  def renderLocalDate(columnName: String, value: LocalDate): CharSequence = "FIXME"

  def renderOffsetDate(columnName: String, value: OffsetDate): CharSequence = "FIXME"

  def renderLocalDateTime(columnName: String, value: LocalDateTime): CharSequence = "FIXME"

  def renderOffsetDateTime(columnName: String, value: OffsetDateTime): CharSequence = "FIXME"

  def renderInterval(columnName: String, value: DateTimeInterval): CharSequence = "FIXME"
}
