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

import quasar.api.Labeled
import quasar.api.push.param._
import quasar.connector.destination.Constructor

import cats.data.Ior

import doobie.Fragment

abstract class HANAType(spec: String) extends Product with Serializable {
  def asSql: Fragment = Fragment.const0(spec)
}

object HANAType {
  case object DATE extends HANATypeId.SelfIdentified("DATE", 1)
  case object TIME extends HANATypeId.SelfIdentified("TIME", 2)
  case object SECONDDATE extends HANATypeId.SelfIdentified("SECONDDATE", 3)
  case object TIMESTAMP extends HANATypeId.SelfIdentified("TIMESTAMP", 4)

  case object TINYINT extends HANATypeId.SelfIdentified("TINYINT", 5)
  case object SMALLINT extends HANATypeId.SelfIdentified("SMALLINT", 6)
  case object INTEGER extends HANATypeId.SelfIdentified("INTEGER", 7)
  case object BIGINT extends HANATypeId.SelfIdentified("BIGINT", 8)
  case object SMALLDECIMAL extends HANATypeId.SelfIdentified("SMALLDECIMAL", 9)
  case object REAL extends HANATypeId.SelfIdentified("REAL", 10)
  case object DOUBLE extends HANATypeId.SelfIdentified("DOUBLE", 11)

  final case class DECIMAL(precision: Int, scale: Int) extends HANAType(s"DECIMAL($precision, $scale)")
  case object DECIMAL extends HANATypeId.HigherKinded(12) {
    val constructor = Constructor.Binary(LengthPrecisionParam(38), LengthScaleParam(38), DECIMAL(_, _))
  }

  final case class FLOAT(sig: Int) extends HANAType(s"FLOAT($sig)")
  case object FLOAT extends HANATypeId.HigherKinded(13) {
    val constructor = Constructor.Unary(LengthFloatParam(53), FLOAT(_))
  }

  case object BOOLEAN extends HANATypeId.SelfIdentified("BOOLEAN", 14)

  final case class VARCHAR(length: Int) extends HANAType(s"VARCHAR($length)")
  case object VARCHAR extends HANATypeId.HigherKinded(15) {
    val constructor = Constructor.Unary(LengthCharParam(5000), VARCHAR(_))
  }

  final case class NVARCHAR(length: Int) extends HANAType(s"NVARCHAR($length)")
  case object NVARCHAR extends HANATypeId.HigherKinded(16) {
    val constructor = Constructor.Unary(LengthCharParam(5000), NVARCHAR(_))
  }

  final case class ALPHANUM(length: Int) extends HANAType(s"ALPHANUM($length)")
  case object ALPHANUM extends HANATypeId.HigherKinded(17) {
    val constructor = Constructor.Unary(LengthCharParam(5000), ALPHANUM(_))
  }

  final case class SHORTTEXT(length: Int) extends HANAType(s"SHORTTEXT($length)")
  case object SHORTTEXT extends HANATypeId.HigherKinded(18) {
    val constructor = Constructor.Unary(LengthCharParam(5000), SHORTTEXT(_))
  }

  final case class VARBINARY(length: Int) extends HANAType(s"VARBINARY($length)")
  case object VARBINARY extends HANATypeId.HigherKinded(19) {
    val constructor = Constructor.Unary(LengthBinaryParam(5000), VARBINARY(_))
  }

  case object BLOB extends HANATypeId.SelfIdentified("BLOB", 20)
  case object CLOB extends HANATypeId.SelfIdentified("CLOB", 21)
  case object NCLOB extends HANATypeId.SelfIdentified("NCLOB", 22)
  case object TEXT extends HANATypeId.SelfIdentified("TEXT", 23)

  ////

  private def LengthFloatParam(max: Int): Labeled[Formal[Int]] =
    Labeled("Significant bits", Formal.integer(Some(Ior.both(1, max)), None, None))

  private def LengthPrecisionParam(max: Int): Labeled[Formal[Int]] =
    Labeled("Decimal precision", Formal.integer(Some(Ior.both(1, max)), None, None))

  private def LengthScaleParam(max: Int): Labeled[Formal[Int]] =
    Labeled("Decimal scale", Formal.integer(Some(Ior.both(1, max)), None, None))

  private def LengthCharParam(max: Int): Labeled[Formal[Int]] =
    Labeled("Length (characters)", Formal.integer(Some(Ior.both(1, max)), None, None))

  private def LengthBinaryParam(max: Int): Labeled[Formal[Int]] =
    Labeled("Length (bytes)", Formal.integer(Some(Ior.both(1, max)), None, None))
}

sealed trait HANATypeId extends Product with Serializable {
  def ordinal: Int
}

object HANATypeId {
  import HANAType._

  sealed abstract class SelfIdentified(spec: String, val ordinal: Int)
      extends HANAType(spec) with HANATypeId

  sealed abstract class HigherKinded(val ordinal: Int) extends HANATypeId {
    def constructor: Constructor[HANAType]
  }

  val allIds: Set[HANATypeId] =
    Set(
      DATE,
      TIME,
      SECONDDATE,
      TIMESTAMP,

      TINYINT,
      SMALLINT,
      INTEGER,
      BIGINT,
      SMALLDECIMAL,
      DECIMAL,
      REAL,
      DOUBLE,

      BOOLEAN,

      VARCHAR, // ASCII text
      NVARCHAR, // Unicode text
      ALPHANUM, // alpha-numeric text
      SHORTTEXT, // searchable text, yields column of type NVARCHAR

      VARBINARY,

      BLOB, // binary large object
      CLOB, // ASCII large object
      NCLOB, // Unicode large object
      TEXT, // searchable text, convertable to (N)VARCHAR
    )
}
