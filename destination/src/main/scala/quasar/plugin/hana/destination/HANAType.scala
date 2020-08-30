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

import quasar.connector.destination.Constructor

import doobie.Fragment

abstract class HANAType(spec: String) extends Product with Serializable {
  def asSql: Fragment = Fragment.const0(spec)
}

// TODO higher kindedness
object HANAType {
  case object DATE extends HANATypeId.SelfIdentified("DATE", 0)
  case object TIME extends HANATypeId.SelfIdentified("IME", 0)
  case object SECONDDATE extends HANATypeId.SelfIdentified("SECONDDATE", 0)
  case object TIMESTAMP extends HANATypeId.SelfIdentified("TIMESTAMP", 0)

  case object TINYINT extends HANATypeId.SelfIdentified("TINYINT", 0)
  case object SMALLINT extends HANATypeId.SelfIdentified("SMALLINT", 0)
  case object INTEGER extends HANATypeId.SelfIdentified("INTEGER", 0)
  case object BIGINT extends HANATypeId.SelfIdentified("BIGINT", 0)
  case object SMALLDECIMAL extends HANATypeId.SelfIdentified("SMALLDECIMAL", 0)
  case object DECIMAL extends HANATypeId.SelfIdentified("DECIMAL", 0)
  case object REAL extends HANATypeId.SelfIdentified("REAL", 0)
  case object DOUBLE extends HANATypeId.SelfIdentified("DOUBLE", 0)

  case object BOOLEAN extends HANATypeId.SelfIdentified("BOOLEAN", 0)

  case object VARCHAR extends HANATypeId.SelfIdentified("VARCHAR", 0)
  case object NVARCHAR extends HANATypeId.SelfIdentified("NVARCHAR", 0)
  case object ALPHANUM extends HANATypeId.SelfIdentified("ALPHANUM", 0)
  case object SHORTTEXT extends HANATypeId.SelfIdentified("SHORTTEXT", 0)

  case object VARBINARY extends HANATypeId.SelfIdentified("VARBINARY", 0)

  case object BLOB extends HANATypeId.SelfIdentified("BLOB", 0)
  case object CLOB extends HANATypeId.SelfIdentified("CLOB", 0)
  case object NCLOB extends HANATypeId.SelfIdentified("NCLOB", 0)
  case object TEXT extends HANATypeId.SelfIdentified("TEXT", 0)

  case object ARRAY extends HANATypeId.SelfIdentified("ARRAY", 0)

  case object ST_GEOMETRY extends HANATypeId.SelfIdentified("ST_GEOMETRY", 0)
  case object ST_POINT extends HANATypeId.SelfIdentified("ST_POINT", 0)
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

      VARCHAR,
      NVARCHAR,
      ALPHANUM,
      SHORTTEXT,

      VARBINARY,

      BLOB,
      CLOB,
      NCLOB,
      TEXT,

      ARRAY,

      ST_GEOMETRY,
      ST_POINT
    )
}
