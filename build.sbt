name := "twitter-stats"

version := "0.1"

scalaVersion := "2.12.9"

val http4sVersion = "0.21.0-SNAPSHOT"

// Only necessary for SNAPSHOT releases
resolvers ++= Seq(
  Resolver.sonatypeRepo("snapshots"),
)

libraryDependencies ++= Seq(
  "org.http4s" %% "http4s-blaze-client" % http4sVersion,
  "org.http4s" %% "http4s-blaze-server" % http4sVersion,
  "org.http4s" %% "http4s-dsl" % http4sVersion,
  "org.http4s" %% "http4s-circe" % http4sVersion,
  "io.circe" %% "circe-generic" % "0.12.0-RC3",
  "org.typelevel" %% "kittens" % "1.2.1",
  "com.vdurmont" % "emoji-java" % "5.0.0",

  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
)

scalacOptions ++= Seq("-Ypartial-unification")

