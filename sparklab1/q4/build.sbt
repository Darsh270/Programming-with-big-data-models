lazy val root = (project in file("."))
.settings(
name := "retail1",
version := "4.0", //version of *your* code
scalaVersion := "2.12.17"
)
libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "3.4.1" % "provided", // change this
"org.apache.spark" %% "spark-sql" % "3.4.1" % "provided" // change this
)
