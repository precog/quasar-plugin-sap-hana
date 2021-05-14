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

import slamdata.Predef._

import quasar.api.resource._
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.contrib.scalaz.MonadError_

import cats.effect._
import cats.effect.testing.specs2.CatsIO

import doobie._
import doobie.implicits._

import org.specs2.mutable.Specification

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.sys
import scala.util.Random

trait TestHarness extends Specification with CatsIO {
  override val Timeout = 60.seconds

  implicit val ioMonadResourceErr: MonadResourceErr[IO] =
    MonadError_.facet[IO](ResourceError.throwableP)

  val frag = Fragment.const0(_, None)

  def TestUrl: String = sys.env.get("SAP_HANA_JDBC") getOrElse ""

  def TestXa(jdbcUrl: String): Resource[IO, Transactor[IO]] =
    Resource.make(IO(Executors.newSingleThreadExecutor()))(p => IO(p.shutdown)) map { ex =>
      Transactor.fromDriverManager[IO](
        "com.sap.db.jdbc.Driver",
        jdbcUrl,
        Blocker.liftExecutionContext(ExecutionContext.fromExecutorService(ex)))
    }

  def table(
      xa: Transactor[IO],
      specialString: String = "")
      : Resource[IO, (ResourcePath, String)] = {
    val setup = Resource.make(
      // Note, that this is only for tests, actual identifiers are cleaned via HANAHygiene
      IO(s"HANA_SPEC_${Random.alphanumeric.take(6).mkString.toUpperCase}" + specialString))(
      name => {
        frag(s"DROP TABLE $name").update.run.transact(xa).void
      })

    setup map { n =>
      (ResourcePath.root() / ResourceName(n), n)
    }
  }

  def tableHarness(
      jdbcUrl: String = TestUrl,
      specialString: String = "")
      : Resource[IO, (Transactor[IO], ResourcePath, String)] =
    for {
      xa <- TestXa(jdbcUrl)
      (path, name) <- table(xa, specialString)
    } yield (xa, path, name)
}
