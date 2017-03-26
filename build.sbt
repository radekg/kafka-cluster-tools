name := "kafka-cluster-tools"

version := "1.0"

scalaVersion := "2.12.1"

enablePlugins(AutomateHeaderPlugin)
enablePlugins(SbtScalariform)

libraryDependencies ++= DependencyGroups.configuration
libraryDependencies ++= DependencyGroups.logging
libraryDependencies ++= DependencyGroups.kafka
libraryDependencies ++= DependencyGroups.kafkaTestTools
libraryDependencies ++= Seq(Library.msgPack, Library.scalaJava8Compat)
libraryDependencies ++= DependencyGroups.unitTests
libraryDependencies ++= DependencyGroups.zookeeper

crossPaths := false
testOptions in Test := Seq(Tests.Argument(TestFrameworks.JUnit, "-a"))