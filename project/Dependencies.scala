import sbt._

object Version {
  val apacheCurator = "2.12.0"
  val kafka = "0.10.2.0"
  val logbackClassic = "1.1.3"
  val msgPack = "0.8.12"
  val scalaLogging = "3.5.0"
  val scalaTest = "3.0.1"
  val slf4j = "1.7.22"
  val typesafeConfig = "1.3.1"
  val zookeeper = "3.4.9"
}

object Exclusions {
  val excludeAll = Seq(ExclusionRule("log4j", "log4j"), ExclusionRule("org.slf4j", "slf4j-log4j12"))
}

object Library {
  val apacheCurator = "org.apache.curator" % "curator-test" % Version.apacheCurator excludeAll(Exclusions.excludeAll: _*)
  val kafka: ModuleID = "org.apache.kafka" %% "kafka" % Version.kafka excludeAll(Exclusions.excludeAll: _*)
  val kafkaClients: ModuleID = "org.apache.kafka" % "kafka-clients" % Version.kafka excludeAll(Exclusions.excludeAll: _*)
  val logbackClassic: ModuleID = "ch.qos.logback" % "logback-classic" % Version.logbackClassic
  val msgPack: ModuleID = "org.msgpack" % "msgpack-core" % Version.msgPack
  val scalaLogging: ModuleID = "com.typesafe.scala-logging" % "scala-logging_2.11" % Version.scalaLogging
  val scalaTest: ModuleID =  "org.scalatest" %% "scalatest" % Version.scalaTest excludeAll(Exclusions.excludeAll: _*)
  val slf4j: ModuleID = "org.slf4j" % "log4j-over-slf4j" % Version.slf4j
  val typesafeConfig: ModuleID = "com.typesafe" % "config" % Version.typesafeConfig excludeAll(Exclusions.excludeAll: _*)
  val zookeeper: ModuleID = "org.apache.zookeeper" % "zookeeper" % Version.zookeeper excludeAll(Exclusions.excludeAll: _*)
}

object DependencyGroups {
  val configuration = Seq(Library.typesafeConfig)
  val kafka = Seq(Library.kafka, Library.kafkaClients)
  val kafkaTestTools = Seq(Library.kafka classifier "test", Library.kafkaClients classifier "test")
  val logging = Seq(Library.scalaLogging, Library.slf4j, Library.logbackClassic)
  val zookeeper = Seq(Library.apacheCurator, Library.zookeeper)
  val unitTests = Seq(Library.scalaTest % "test")
}