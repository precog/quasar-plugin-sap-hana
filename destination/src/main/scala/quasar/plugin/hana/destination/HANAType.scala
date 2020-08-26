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

object HANAType {

  case object BOOLEAN extends HANATypeId.SelfIdentified("BOOLEAN", 0)
}

sealed trait HANATypeId extends Product with Serializable {
  def ordinal: Int
}

object HANATypeId {

  sealed abstract class SelfIdentified(spec: String, val ordinal: Int)
      extends HANAType(spec) with HANATypeId

  sealed abstract class HigherKinded(val ordinal: Int) extends HANATypeId {
    def constructor: Constructor[HANAType]
  }

  val allIds: Set[HANATypeId] = Set(HANAType.BOOLEAN)
}
