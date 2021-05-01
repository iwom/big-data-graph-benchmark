name := "flink-core"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.flink" % "flink-core" % "1.10.0",
  "org.apache.flink" % "flink-streaming-scala_2.11" % "1.10.0"
)

fork in run := true