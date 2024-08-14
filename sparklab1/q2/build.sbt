lazy val root = (project in file("."))
.settings(
name := "wordcount1",
version := "2.0", //version of *your* code
scalaVersion := "2.12.17"
)
libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "3.4.1" % "provided", // change this
"org.apache.spark" %% "spark-sql" % "3.4.1" % "provided" // change this
)
