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

  def renderUndefined(columnName: String): CharSequence = ""

  def renderNull(columnName: String): CharSequence = ""

  def renderEmptyArray(columnName: String): CharSequence = ""

  def renderEmptyObject(columnName: String): CharSequence = ""

  def renderBoolean(columnName: String, value: Boolean): CharSequence = ""

  def renderLong(columnName: String, value: Long): CharSequence = ""

  def renderDouble(columnName: String, value: Double): CharSequence = ""

  def renderBigDecimal(columnName: String, value: BigDecimal): CharSequence = ""

  def renderString(columnName: String, value: String): CharSequence = ""

  def renderLocalTime(columnName: String, value: LocalTime): CharSequence = ""

  def renderOffsetTime(columnName: String, value: OffsetTime): CharSequence = ""

  def renderLocalDate(columnName: String, value: LocalDate): CharSequence = ""

  def renderOffsetDate(columnName: String, value: OffsetDate): CharSequence = ""

  def renderLocalDateTime(columnName: String, value: LocalDateTime): CharSequence = ""

  def renderOffsetDateTime(columnName: String, value: OffsetDateTime): CharSequence = ""

  def renderInterval(columnName: String, value: DateTimeInterval): CharSequence = ""
}
