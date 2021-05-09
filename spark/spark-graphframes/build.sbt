name := "spark-graphframes"

version := "0.1"

scalaVersion := "2.12.10"

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.0.0"
libraryDependencies += "graphframes" % "graphframes" % "0.8.1-spark3.0-s_2.12"

fork in run := true

assemblyJarName in assembly := "spark-graphframes.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
