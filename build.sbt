name := "kafka-cluster-tools"
organization := "com.gruchalski"
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

coverageEnabled in Test := true

publishMavenStyle := true

pomIncludeRepository := { _ => false }

pomExtra := (
  <url>https://github.com/radekg/kafka-cluster-utils</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:git://github.com/radekg/kafka-cluster-utils.git</connection>
      <developerConnection>scm:git:git://github.com/radekg/kafka-cluster-utils.git</developerConnection>
      <url>https://github.com/radekg/kafka-cluster-utils/tree/master</url>
    </scm>
    <developers>
      <developer>
        <id>radekg</id>
        <name>Radoslaw Gruchalski</name>
        <url>http://gruchalski.com</url>
      </developer>
    </developers>
  )