/*
 * Copyright 2017 Rad Gruchalski
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

package com.gruchalski.typesafe.config

import java.util.Properties

import com.typesafe.config.Config

/**
 * Extensions for Typesafe Config.
 */
object ConfigImplicits {

  /**
   * Typesafe config implicits.
   * @param config Typesafe config
   */
  implicit class ConfigExtensions(config: Config) {
    import com.gruchalski.utils.StringImplicits.StringExtensions

    import scala.collection.JavaConverters._
    /**
     * Convert a config to Properties.
     * <p>
     *   Iterates over the keys, unquotes the key and calls toString on the unwrapped item for every key.
     * @return config converted to properties
     */
    def toProperties(): Properties = {
      config.entrySet().asScala.foldLeft(new Properties()) { (accum, item) ⇒
        accum.setProperty(item.getKey.unquoted(), item.getValue.unwrapped().toString)
        accum
      }
    }
  }
}
