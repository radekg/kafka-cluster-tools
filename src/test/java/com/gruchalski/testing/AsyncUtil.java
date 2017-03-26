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

package com.gruchalski.testing;

import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.Assert.fail;

public class AsyncUtil {

    protected AsyncUtil() {}

    private static ExecutorService executor = Executors.newSingleThreadExecutor();
    private static ConcurrentHashMap<UUID, AssertionError> errorMap = new ConcurrentHashMap<>();

    public static void eventually(Runnable r) {
        eventually(r, 10000);
    }

    public static void eventually(Runnable r, long timeout) {
        eventually(r, timeout, 100);
    }

    public static void eventually(Runnable r, long timeout, long interval) {
        UUID execution = UUID.randomUUID();
        long end = System.currentTimeMillis() + timeout;
        long tries = 0;
        long failed = 0;
        boolean success = false;
        while (System.currentTimeMillis() < end) {
            tries++;
            Future<Void> f = executor.submit(new Invocation(r, execution));
            try {
                f.get(timeout, TimeUnit.MILLISECONDS);
                if (errorMap.containsKey(execution)) {
                    throw errorMap.get(execution);
                }
                success = true;
                break;
            } catch (InterruptedException ex) {
                failed++;
                try { Thread.sleep(interval); } catch (InterruptedException ex2) {}
            } catch (ExecutionException ex) {
                failed++;
                try { Thread.sleep(interval); } catch (InterruptedException ex2) {}
            } catch (TimeoutException ex) {
                failed++;
                try { Thread.sleep(interval); } catch (InterruptedException ex2) {}
            } catch (AssertionError ex) {
                failed++;
                try { Thread.sleep(interval); } catch (InterruptedException ex2) {}
            } finally {
                errorMap.remove(execution);
            }
        }
        if (!success) {
            fail("Eventually executed " + tries + " times and failed " + failed + " times after " + timeout + " milliseconds.");
        }
    }

    private static class Invocation implements Callable<Void> {
        private Runnable task;
        private UUID execution;
        public Invocation(Runnable r, UUID execution) {
            task = r;
            execution = execution;
        }
        public Void call() {
            InvocationUncaughtExceptionHandler handler = new InvocationUncaughtExceptionHandler();
            Thread t = new Thread(task);
            t.setUncaughtExceptionHandler(handler);
            t.start();
            try { t.join(); } catch (InterruptedException ex) {}
            if (handler.hadException()) {
                errorMap.put(execution, handler.getException());
            }
            return null;
        }
    }

    private static class InvocationUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
        private AssertionError ae;
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            if (e.getClass().isAssignableFrom(AssertionError.class)) {
                ae = (AssertionError) e;
            }
        }
        public boolean hadException() {
            return ae != null;
        }
        public AssertionError getException() {
            return ae;
        }
    }

}
