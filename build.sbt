ThisBuild / crossScalaVersions := Seq("2.12.10")
ThisBuild / scalaVersion := (ThisBuild / crossScalaVersions).value.head

ThisBuild / githubRepository := "quasar-plugin-sap-hana"

ThisBuild / homepage := Some(url("https://github.com/precog/quasar-plugin-sap-hana"))

ThisBuild / scmInfo := Some(ScmInfo(
  url("https://github.com/precog/quasar-plugin-sap-hana"),
  "scm:git@github.com:precog/quasar-plugin-sap-hana.git"))

ThisBuild / publishAsOSSProject := true

lazy val quasarVersion =
  Def.setting[String](managedVersions.value("precog-quasar"))

lazy val quasarPluginJdbcVersion =
  Def.setting[String](managedVersions.value("precog-quasar-plugin-jdbc"))

val sapVersion = "2.5.50"
val specs2Version = "4.9.4"

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core, destination)

lazy val core = project
  .in(file("core"))
  .settings(
    name := "quasar-plugin-sap-hana",

    libraryDependencies ++= Seq(
      "com.precog" %% "quasar-plugin-jdbc" % quasarPluginJdbcVersion.value,
      "org.specs2" %% "specs2-core"        % specs2Version % Test
    ))

lazy val destination = project
  .in(file("destination"))
  .dependsOn(core % BothScopes)
  .settings(
    name := "quasar-destination-hana",

    quasarPluginName := "hana",
    quasarPluginQuasarVersion := quasarVersion.value,
    quasarPluginDestinationFqcn := Some("quasar.plugin.hana.destination.HANADestinationModule$"),

    quasarPluginDependencies ++= Seq(
      "com.precog"            %% "quasar-plugin-jdbc" % quasarPluginJdbcVersion.value,
      "com.sap.cloud.db.jdbc"  % "ngdbc"              % sapVersion pomOnly()
    ))
  .enablePlugins(QuasarPlugin)
