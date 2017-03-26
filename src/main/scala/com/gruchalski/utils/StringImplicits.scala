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

package com.gruchalski.utils

/**
 * String extensions.
 */
object StringImplicits {

  /**
   * String implicits.
   * @param string a string
   */
  implicit class StringExtensions(val string: String) {

    /**
     * Remove leading and trailing quotes, if any.
     * @return a string without leading and trailing quotes
     */
    def unquoted(): String = string.replaceAll("^\"|\"$", "")
  }

}
