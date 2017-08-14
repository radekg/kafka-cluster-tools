name := "kafka-cluster-tools"
organization := "com.gruchalski"
scalaVersion := "2.12.3"
crossScalaVersions := Seq("2.11.11", "2.12.3")

lazy val root = project
  .in(file("."))
  .enablePlugins(AutomateHeaderPlugin, SbtScalariform)
  .settings(
    libraryDependencies ++= DependencyGroups.configuration
      ++ DependencyGroups.logging
      ++ DependencyGroups.kafka
      ++ DependencyGroups.kafkaTestTools
      ++ Seq(Library.msgPack, Library.scalaJava8Compat)
      ++ DependencyGroups.unitTests
      ++ DependencyGroups.zookeeper,
    crossPaths := true,
    parallelExecution in ThisBuild := false,
    testOptions in Test := Seq(Tests.Argument(TestFrameworks.JUnit, "-a")),
    coverageEnabled in Test := true,
    publishMavenStyle := true,
    pomIncludeRepository := { _ => false },
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
  )