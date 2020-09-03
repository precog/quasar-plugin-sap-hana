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

import scala._
import scala.concurrent.duration._

import argonaut._, Argonaut._

import org.specs2.mutable.Specification

object ConnectionConfigSpec extends Specification {

  import ConnectionConfig.Redacted

  "serialization" >> {
    "valid config" >> {
      val js = """
        {
          "jdbcUrl": "jdbc:sap://example.com/db2?user=alice&password=secret&compress=true",
          "maxConcurrency": 4,
          "maxLifetimeSecs": 180
        }
      """

      val expected =
        ConnectionConfig(
          "jdbc:sap://example.com/db2",
          List(
            DriverParameter("user", "alice"),
            DriverParameter("password", "secret"),
            DriverParameter("compress", "true")),
          Some(4),
          Some(3.minutes))

      js.decodeEither[ConnectionConfig] must beRight(expected)
    }

    "url parameters are optional" >> {
      val base = "jdbc:sap://example.com:1234/db"

      val js = s"""
        {
          "jdbcUrl": "$base",
          "maxConcurrency": 4,
          "maxLifetimeSecs": 180
        }
      """

      val expected =
        ConnectionConfig(
          base,
          Nil,
          Some(4),
          Some(3.minutes))

      js.decodeEither[ConnectionConfig] must beRight(expected)
    }

    "max concurrency is optional" >> {
      val js = """
        {
          "jdbcUrl": "jdbc:sap://example.com/db2?user=alice&password=secret&compress=true",
          "maxLifetimeSecs": 180
        }
      """

      val expected =
        ConnectionConfig(
          "jdbc:sap://example.com/db2",
          List(
            DriverParameter("user", "alice"),
            DriverParameter("password", "secret"),
            DriverParameter("compress", "true")),
          None,
          Some(3.minutes))

      js.decodeEither[ConnectionConfig] must beRight(expected)
    }

    "max lifetime is optional" >> {
      val js = """
        {
          "jdbcUrl": "jdbc:sap://example.com/db2?user=alice&password=secret&compress=true",
          "maxConcurrency": 4
        }
      """

      val expected =
        ConnectionConfig(
          "jdbc:sap://example.com/db2",
          List(
            DriverParameter("user", "alice"),
            DriverParameter("password", "secret"),
            DriverParameter("compress", "true")),
          Some(4),
          None)

      js.decodeEither[ConnectionConfig] must beRight(expected)
    }

    "fails when parameters malformed" >> {
      val js = """
        {
          "jdbcUrl": "jdbc:sap://example.com/db2?=alice&password=secret&compress=true",
          "maxConcurrency": 4,
          "maxLifetimeSecs": 180
        }
      """

      js.decodeEither[ConnectionConfig] must beLeft(contain("Malformed driver parameter"))
    }
  }

  "sanitization" >> {
    "sanitizes password parameters" >> {
      val cc =
        ConnectionConfig(
          "jdbc:sap://example.com/db",
          List(
            DriverParameter("password", "secret1"),
            DriverParameter("cseKeyStorePassword", "secret2"),
            DriverParameter("keyStorePassword", "secret3"),
            DriverParameter("trustStorePassword", "secret4"),
            DriverParameter("proxyPassword", "secret5"),
            DriverParameter("user", "bob")),
          None,
          None)

      val expected =
        ConnectionConfig(
          "jdbc:sap://example.com/db",
          List(
            DriverParameter("password", Redacted),
            DriverParameter("cseKeyStorePassword", Redacted),
            DriverParameter("keyStorePassword", Redacted),
            DriverParameter("trustStorePassword", Redacted),
            DriverParameter("proxyPassword", Redacted),
            DriverParameter("user", "bob")),
          None,
          None)

      cc.sanitized must_=== expected
    }
  }

  "validation" >> {
    "fails when a denied parameter is present" >> {
      val cc =
        ConnectionConfig(
          "jdbc:sap://example.com/db",
          List(
            DriverParameter("password", "nopeek"),
            DriverParameter("user", "bob"),
            DriverParameter("autocommit", "true")),
          Some(3),
          None)

      cc.validated.toEither must beLeft("Unsupported parameters: autocommit")
    }

    "fails when multiple denied parameters are present" >> {
      val cc =
        ConnectionConfig(
          "jdbc:sap://example.com/db",
          List(
            DriverParameter("password", "nopeek"),
            DriverParameter("latency", "true"),
            DriverParameter("user", "bob"),
            DriverParameter("autocommit", "true")),
          Some(3),
          None)

      cc.validated.toEither must beLeft("Unsupported parameters: latency, autocommit")
    }

    "succeeds when no parameters are denied" >> {
      val cc =
        ConnectionConfig(
          "jdbc:sap://example.com/db",
          List(
            DriverParameter("password", "nopeek"),
            DriverParameter("user", "bob"),
            DriverParameter("compress", "true")),
          Some(3),
          None)

      cc.validated.toEither must beRight(cc)
    }
  }

  "merge sensitive" >> {
    "merges undefined sensitive params from other" >> {
      val a = ConnectionConfig(
        "jdbc:sap://example.com/db",
        List(
          DriverParameter("user", "alice"),
          DriverParameter("compress", "true")),
        None,
        None)

      val b = ConnectionConfig(
        "jdbc:sap://example.com/db",
        List(
          DriverParameter("user", "bob"),
          DriverParameter("password", "secret"),
          DriverParameter("compress", "false")),
        None,
        None)

      val expected =
        ConnectionConfig(
          "jdbc:sap://example.com/db",
          List(
            DriverParameter("password", "secret"),
            DriverParameter("user", "alice"),
            DriverParameter("compress", "true")),
          None,
          None)

      a.mergeSensitive(b) must_=== expected
    }

    "retains local sensitive params" >> {
      val a = ConnectionConfig(
        "jdbc:sap://example.com/db",
        List(
          DriverParameter("user", "alice"),
          DriverParameter("password", "toor"),
          DriverParameter("compress", "true")),
        None,
        None)

      val b = ConnectionConfig(
        "jdbc:sap://example.com/db",
        List(
          DriverParameter("user", "bob"),
          DriverParameter("password", "secret"),
          DriverParameter("keyStorePassword", "hiddenkeys"),
          DriverParameter("compress", "false")),
        None,
        None)

      val expected =
        ConnectionConfig(
          "jdbc:sap://example.com/db",
          List(
            DriverParameter("keyStorePassword", "hiddenkeys"),
            DriverParameter("user", "alice"),
            DriverParameter("password", "toor"),
            DriverParameter("compress", "true")),
          None,
          None)

      a.mergeSensitive(b) must_=== expected
    }
  }
}
