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

package com.gruchalski.kafka.java8.compat;

/**
 * Compatibility with Java.
 */
public class JavaCompat {
  
  /**
   * Convert Java Iterator to Scala Iterator.
   * @param iter Java iterator to convert to Scala iterator
   * @param <T> iterator type
   * @return scala iterator
   */
  public static <T> scala.collection.Iterator<T> asScalaIterator(java.util.Iterator<T> iter) {
    scala.collection.mutable.ArrayBuffer<T> buf = new scala.collection.mutable.ArrayBuffer<T>();
    while (iter.hasNext()) {
      buf = buf.$plus$eq$colon(iter.next());
    }
    return buf.iterator();
  }
  
}
