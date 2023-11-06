ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "rdb-generator"
  )

libraryDependencies += "com.moilioncircle" % "redis-replicator" % "3.5.5" //Thanks for using https://jar-download.com