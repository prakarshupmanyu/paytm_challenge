name := "paytmchallenge"

version := "0.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.4.7"
val sqlVersion = "2.4.4"


libraryDependencies += "org.scala-lang" % "scala-library" % scalaVersion.value % "test"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sqlVersion
)