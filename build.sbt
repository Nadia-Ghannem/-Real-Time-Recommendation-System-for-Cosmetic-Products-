ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "projectsparkFN"
  )

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.7.0",
  "org.apache.kafka" % "kafka-clients" % "3.7.0",
  "org.json4s" %% "json4s-core" % "3.6.7",
  "org.json4s" %% "json4s-native" % "3.6.7", // Inclusion explicite de json4s-jackson
  "ch.qos.logback" % "logback-classic" % "1.5.6" % Test,
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-mllib" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.apache.spark" %% "spark-streaming" % "3.5.1",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.5.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.0",
  "org.apache.httpcomponents" % "httpclient" % "4.5.13",
  "log4j" % "log4j" % "1.2.17" ,
  //"org.slf4j" % "slf4j-api" % "2.0.12",
  //""org.slf4j" % "slf4j-log4j12" % "2.0.11" % Test pomOnly()"
)





// Importer les configurations nécessaires pour sbt-assembly
import sbtassembly.AssemblyPlugin.autoImport._

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
resolvers += "Confluent Maven Repo" at "https://packages.confluent.io/maven/"

//resolvers += "Confluent Maven Repo" at "https://packages.confluent.io/maven/"
