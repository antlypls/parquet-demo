name := "parquet-demo"

scalaVersion := "2.11.7"

resolvers ++= Seq(
  "Twitter Maven Repo" at "http://maven.twttr.com"
)

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk" % "1.7.5",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.1",
  "org.apache.hadoop" % "hadoop-common" % "2.7.1",
  "org.apache.parquet" % "parquet-protobuf" % "1.8.1"
)
