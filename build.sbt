organization := "com.typeduke"

name := "scala-sbt-codespaces-template"

version := "0.0.1"

scalaVersion := "3.3.1"

resolvers += "Akka library repository".at("https://repo.akka.io/maven")

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.17" % "test"

val AkkaVersion = "2.10.5"
val AkkaHttpVersion = "10.7.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion, // (*1)
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion // (*2)
)

libraryDependencies ++= Seq(
  "io.lettuce" % "lettuce-core" % "6.7.1.RELEASE"
)
