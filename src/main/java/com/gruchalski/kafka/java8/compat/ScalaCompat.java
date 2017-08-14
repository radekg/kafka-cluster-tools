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

import scala.runtime.AbstractFunction1;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Compatibility with Scala.
 */
public class ScalaCompat {

    /**
     * Convert Scala Option to Java Optional.
     * @param opt option
     * @param <T> type of the option
     * @return optional
     */
    public static <T> Optional fromScala(scala.Option<T> opt) {
        return Optional.ofNullable(opt.get());
    }

    /**
     * Convert Scala Future to Java CompletableFuture.
     * @param fut Scala future
     * @param <T> future type
     * @return Java CompletableFuture
     */
    public static <T> CompletableFuture fromScala(scala.concurrent.Future<T> fut) {
        return scala.compat.java8.FutureConverters.toJava(fut).toCompletableFuture();
    }

    /**
     * Convert a Scala List of Scala Futures into an array of CompletableFutures.
     * @param lfut Scala List of Scala futures
     * @param <T> future type
     * @return array of Java CompletableFuture
     */
    public static <T> CompletableFuture[] fromScala(scala.collection.immutable.List<scala.concurrent.Future<T>> lfut) {
        CompletableFuture<T> arrItems[] = new CompletableFuture[lfut.size()];
        for (int i=0; i<lfut.length(); i++) {
            arrItems[i] = ScalaCompat.fromScala(lfut.apply(i));
        }
        return arrItems;
    }
    
    public static <T> java.util.Iterator<T> asJavaIterator(scala.collection.Iterator<T> iter) {
        ArrayList<T> list = new ArrayList<>();
        iter.foreach(new AbstractFunction1<T, Object>() {
            public T apply(T item) {
                return item;
            }
        });
        return list.iterator();
    }
    
    public static <T> java.util.List<T> seqAsJavaList(scala.collection.Seq<T> seq) {
        ArrayList<T> list = new ArrayList<>();
        seq.foreach(new AbstractFunction1<T, Object>() {
            public T apply(T item) {
                return item;
            }
        });
        return list;
    }

}
