/*
 * Copyright 2017 Radek Gruchalski
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gruchalski.kafka.scala

import com.typesafe.scalalogging.Logger
import org.apache.curator.test.{InstanceSpec, TestingCluster}

import scala.util.Try

/**
 * Embedded ZooKeeper companion object.
 */
object EmbeddedZooKeeper {
  type ConnectionString = String
  case class EmbeddedZooKeeperData(instances: List[InstanceSpec], connectionString: ConnectionString)
  def apply(configuration: Configuration) = new EmbeddedZooKeeper(configuration)
}

/**
 * Embedded ZooKeeper cluster.
 * @param configuration configuration
 */
class EmbeddedZooKeeper(configuration: Configuration) {

  private val logger = Logger(getClass)
  private var cluster: Option[TestingCluster] = None

  /**
   * Start the cluster. Operation is idempotent.
   * @return a tuple containing the instance specs and connection string of the cluster
   */
  def start(): Option[EmbeddedZooKeeper.EmbeddedZooKeeperData] = {
    import collection.JavaConverters._
    logger.info(s"Attempting starting a ZooKeeper cluster of ${configuration.`com.gruchalski.zookeeper.server.ensemble-size`} machines...")
    cluster match {
      case Some(_cluster) ⇒
        logger.warn("Cluster is already running.")
        Some(EmbeddedZooKeeper.EmbeddedZooKeeperData(_cluster.getInstances.asScala.toList, _cluster.getConnectString))
      case None ⇒
        Try {
          val _cluster = new TestingCluster(configuration.`com.gruchalski.zookeeper.server.ensemble-size`)
          _cluster.start()
          cluster = Some(_cluster)
          logger.info("Cluster is now running.")
          Some(EmbeddedZooKeeper.EmbeddedZooKeeperData(_cluster.getInstances.asScala.toList, _cluster.getConnectString))
        }.getOrElse(None)
    }
  }

  /**
   * Stop cluster, if running.
   */
  def stop(): Unit = {
    logger.info("Requesting ZooKeeper cluster to stop.")
    cluster.foreach(_.stop())
    logger.info("ZooKeeper cluster is stopped.")
  }

}
