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

import quasar.plugin.hana._

import scala.{text => _, Stream => _, _}, Predef._
import scala.concurrent.ExecutionContext

import java.lang.CharSequence

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}

import doobie._
import doobie.implicits._

import fs2._

import org.slf4s.Logging

import quasar.api.Column
import quasar.api.resource._
import quasar.connector.destination.ResultSink
import quasar.lib.jdbc.destination.WriteMode

import scala.concurrent.duration._

object HANADestinationSpec extends TestHarness with Logging {
  import HANAType._

  skipAllIf(TestUrl.isEmpty)

  implicit val timer = IO.timer(ExecutionContext.Implicits.global)

  def createSink(
      dest: HANADestination[IO],
      path: ResourcePath,
      cols: NonEmptyList[Column[HANAType]])
      : Pipe[IO, CharSequence, Unit] = {
    val mbSink = dest.sinks.toList.collectFirst { case ResultSink.CreateSink(f) => f }
    mbSink.get.apply(path, cols)._2
  }

  def harnessed(
      jdbcUrl: String = TestUrl,
      writeMode: WriteMode = WriteMode.Replace,
      specialString: String = "")
      : Resource[IO, (Transactor[IO], HANADestination[IO], ResourcePath, String)] =
    tableHarness(jdbcUrl, specialString) map {
      case (xa, path, name) => {
        (xa, new HANADestination(writeMode, Resource.pure[IO, Transactor[IO]](xa), 0, 0.seconds, log), path, name)
      }
    }

  def quote(chars: String): String = s"'$chars'"

  def delim(lines: String*): Stream[IO, CharSequence] =
    Stream.emits(lines)

  def ingestValues[A: Read](tpe: HANAType, values: A*) =
    ingestValuesWithExpected(tpe, values: _*)(values: _*)

  def ingestValuesWithExpected[A: Read](tpe: HANAType, values: A*)(expected: A*) = {
    val input = delim(values.map(_.toString): _*)
    val cols = NonEmptyList.one(Column("value", tpe))

    harnessed(writeMode = WriteMode.Replace) use { case (xa, dest, path, tableName) =>
      for {
        _ <- input.through(createSink(dest, path, cols)).compile.drain
        vals <- frag(s"""select "value" from $tableName""").query[A].to[List].transact(xa)
      } yield {
        vals must containTheSameElementsAs(expected)
      }
    }
  }

  "write mode" >> {
    val cols = NonEmptyList.one(Column("value", VARCHAR(1)))

    "create" >> {
      "succeeds when table absent" >> {
        harnessed(writeMode = WriteMode.Create) use { case (xa, dest, path, tableName) =>
          delim("'A'", "'B'")
            .through(createSink(dest, path, cols))
            .compile.drain.attempt.map(_ must beRight)
        }
      }

      "fails when table present" >> {
        harnessed(writeMode = WriteMode.Create) use { case (xa, dest, path, tableName) =>
          for {
            _ <- delim("'A'", "'B'").through(createSink(dest, path, cols)).compile.drain
            r <- delim("'A'", "'B'").through(createSink(dest, path, cols)).compile.drain.attempt
          } yield {
            r must beLeft
          }
        }
      }
    }

    "replace" >> {
      "succeeds when table absent" >> {
        harnessed(writeMode = WriteMode.Replace) use { case (xa, dest, path, tableName) =>
          delim("'A'", "'B'")
            .through(createSink(dest, path, cols))
            .compile.drain.attempt.map(_ must beRight)
        }
      }

      "succeeds when table present" >> {
        harnessed(writeMode = WriteMode.Replace) use { case (xa, dest, path, tableName) =>
          for {
            _ <- delim("'A'", "'B'").through(createSink(dest, path, cols)).compile.drain
            r <- delim("'A'", "'B'").through(createSink(dest, path, cols)).compile.drain.attempt
          } yield {
            r must beRight
          }
        }
      }
    }

    "truncate" >> {
      "succeeds when table absent" >> {
        harnessed(writeMode = WriteMode.Truncate) use { case (xa, dest, path, tableName) =>
          delim("'A'", "'B'")
            .through(createSink(dest, path, cols))
            .compile.drain.attempt.map(_ must beRight)
        }
      }

      "succeeds when table present" >> {
        harnessed(writeMode = WriteMode.Truncate) use { case (xa, dest, path, tableName) =>
          for {
            _ <- delim("'A'", "'B'").through(createSink(dest, path, cols)).compile.drain
            r <- delim("'A'", "'B'").through(createSink(dest, path, cols)).compile.drain.attempt
          } yield {
            r must beRight
          }
        }
      }
    }

    "append" >> {
      "succeeds when table absent" >> {
        harnessed(writeMode = WriteMode.Append) use { case (xa, dest, path, tableName) =>
          delim("'A'", "'B'")
            .through(createSink(dest, path, cols))
            .compile.drain.attempt.map(_ must beRight)
        }
      }

      "succeeds when table present" >> {
        harnessed(writeMode = WriteMode.Append) use { case (xa, dest, path, tableName) =>
          for {
            _ <- delim("'A'", "'B'").through(createSink(dest, path, cols)).compile.drain
            r <- delim("'A'", "'B'").through(createSink(dest, path, cols)).compile.drain.attempt
          } yield {
            r must beRight
          }
        }
      }
    }
  }

  "ingest" >> {
    "boolean" >> {
      val input = delim("1", "0")
      val cols = NonEmptyList.one(Column("THEBOOL", TINYINT))

      harnessed() use { case (xa, dest, path, tableName) =>
        for {
          _ <- input.through(createSink(dest, path, cols)).compile.drain

          bools <-
            frag(s"select THEBOOL from $tableName")
              .query[Boolean].to[List].transact(xa)
        } yield {
          bools must contain(true, false)
        }
      }
    }

    "number" >> {

      "smallint" >> ingestValues(SMALLINT, -32768, 0, 32767)

      "int" >> ingestValues(INTEGER, -2147483648, 0, 2147483647)

      "bigint" >> ingestValues(BIGINT, -9223372036854775808L, 0L, 9223372036854775807L)

      "float" >> ingestValues(FLOAT(53), 0.0, -2.23E-100, +12.86E+100)


      "decimal" >> {
        val min = BigDecimal("-9999999999999999999999999999.9999999999")
        val mid = BigDecimal("1234499959999999999999999999.0001239999")
        val max = BigDecimal("9999999999999999999999999999.9999999999")
        ingestValues(DECIMAL(38, 10), min, mid, max)
      }
    }

    "string" >> {
      "varchar" >> ingestValuesWithExpected(VARCHAR(6), "'foobar'", "' b a t'")("foobar", " b a t")
    }

    "temporal" >> {
      "date" >> {
        val minDate = "0001-01-01"
        val midDate = "2020-11-09"
        val maxDate = "9999-12-31"

        ingestValuesWithExpected(DATE, quote(minDate), quote(midDate), quote(maxDate))(
          minDate, midDate, maxDate)
      }

      "timestamp" >> {
        val minDateTime = "1753-01-01T00:00:00.000"
        val midDateTime = "2020-11-09T11:13:38.742"
        val midDateTimeNoMs = "2020-11-09T11:13:38"
        val maxDateTime = "9999-12-31T23:59:59.997"

        val expectedMinDateTime = "1753-01-01 00:00:00.000000000"
        val expectedMidDateTime = "2020-11-09 11:13:38.742000000"
        val expectedMidDateTimeNoMs = "2020-11-09 11:13:38.000000000"
        val expectedMaxDateTime = "9999-12-31 23:59:59.997000000"

        ingestValuesWithExpected(TIMESTAMP,
          quote(minDateTime), quote(midDateTime), quote(midDateTimeNoMs), quote(maxDateTime))(
          expectedMinDateTime, expectedMidDateTime, expectedMidDateTimeNoMs, expectedMaxDateTime)
      }
    }

    "containing special characters" >> {
      val escA = "'foo,\",,\"\"'"
      val escB = "'java\"script\"'"
      val escC = "'thisis''stuff'"

      val A = "foo,\",,\"\""
      val B = "java\"script\""
      val C = "thisis'stuff"

      val input = delim(escA, escB, escC)
      val cols = NonEmptyList.one(Column("value", VARCHAR(12)))

      harnessed() use { case (xa, dest, path, tableName) =>
        for {
          _ <- input.through(createSink(dest, path, cols)).compile.drain

          vals <-
            frag(s"""select "value" from $tableName""")
              .query[String].to[List].transact(xa)
        } yield {
          vals must contain(A, B, C)
        }
      }
    }

    "multiple fields with double-quoted string" >> {
      val row1 = "'2020-07-12',123456.234234,'hello world',887798"
      val row2 = "'1983-02-17',732345.987,'lorum, ipsum',42"

      val input = delim(row1, row2)

      val cols: NonEmptyList[Column[HANAType]] = NonEmptyList.of(
        Column("A", DATE),
        Column("B", DECIMAL(12, 6)),
        Column("C", VARCHAR(20)),
        Column("D", INTEGER))

      harnessed() use { case (xa, dest, path, tableName) =>
        for {
          _ <- input.through(createSink(dest, path, cols)).compile.drain

          vals <-
            frag(s"select A, B, C, D from $tableName")
              .query[(String, Double, String, Int)].to[List].transact(xa)
        } yield {
          vals must contain(
            ("2020-07-12", 123456.234234, "hello world", 887798),
            ("1983-02-17", 732345.987, "lorum, ipsum", 42))
        }
      }
    }

    "undefined fields" >> {
      val row1 = s"NULL,42,'1992-03-04'"
      val row2 = s"'foo',NULL,'1992-03-04'"
      val row3 = s"'foo',42,NULL"

      val input = delim(row1, row2, row3)

      val cols = NonEmptyList.of(
        Column("A", VARCHAR(3)),
        Column("B", TINYINT),
        Column("C", DATE))

      harnessed() use { case (xa, dest, path, tableName) =>
        for {
          _ <- input.through(createSink(dest, path, cols)).compile.drain

          vals <-
            frag(s"select A, B, C from $tableName")
              .query[(Option[String], Option[Int], Option[String])]
              .to[List].transact(xa)
        } yield {
          vals must contain(
            (None, Some(42), Some("1992-03-04")),
            (Some("foo"), None, Some("1992-03-04")),
            (Some("foo"), Some(42), None))
        }
      }
    }
  }
}
