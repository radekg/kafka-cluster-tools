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

package com.gruchalski.utils

import scala.util.Try

/**
 * A [[scala.util.Try]] wrapper object providing a version independent [[scala.util.Either]] method.
 * @param underlyingTry [[scala.util.Try]]
 * @tparam T Try type
 * @since 1.4.0
 */
case class TryCompatible[+T](underlyingTry: Try[T]) {
  def toVersionCompatibleEither: Either[Throwable, T] = {
    if (underlyingTry.isSuccess) Right(underlyingTry.get) else Left(underlyingTry.failed.get)
  }
}
