name := "twitter-stats"

version := "0.1"

scalaVersion := "2.13.0"

val http4sVersion = "0.21.0-SNAPSHOT"

// Only necessary for SNAPSHOT releases
resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies ++= Seq(
  "org.http4s" %% "http4s-blaze-client" % http4sVersion,
  "org.http4s" %% "http4s-circe" % http4sVersion,
  "io.circe" %% "circe-generic" % "0.12.0-RC3"
)
