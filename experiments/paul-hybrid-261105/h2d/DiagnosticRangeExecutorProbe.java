/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.fileio.hadoop;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** CPU-only failure, interruption and executor-wide concurrency checks. */
public class DiagnosticRangeExecutorProbe {
    private static void check(boolean ok, String message) {
        if (!ok) {
            throw new AssertionError(message);
        }
    }

    private static void await(CountDownLatch latch) throws IOException {
        try {
            check(latch.await(10, TimeUnit.SECONDS), "Timed out waiting for test worker");
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private static void drainTest(boolean interrupt) throws Exception {
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch failed = new CountDownLatch(1);
        AtomicInteger finished = new AtomicInteger();
        AtomicReference<Throwable> result = new AtomicReference<>();
        Thread caller = new Thread(() -> {
            try {
                DiagnosticRangeExecutor.run(2, lane -> {
                    started.countDown();
                    await(started);
                    if (lane == 0 && !interrupt) {
                        failed.countDown();
                        throw new IOException("Injected read failure");
                    }
                    await(release);
                    finished.incrementAndGet();
                });
                result.set(new AssertionError("Expected read failure or interruption"));
            } catch (Throwable e) {
                result.set(e);
                if (interrupt && !Thread.currentThread().isInterrupted()) {
                    result.set(new AssertionError("Caller interrupt status was lost"));
                }
            }
        });
        caller.start();
        await(started);
        if (interrupt) {
            caller.interrupt();
        } else {
            await(failed);
        }
        caller.join(100);
        check(caller.isAlive(), "Caller returned while a worker still owned its output");
        release.countDown();
        caller.join(10000);
        check(!caller.isAlive(), "Caller did not finish after workers drained");
        check(finished.get() == (interrupt ? 2 : 1), "Read workers did not finish");
        check(interrupt ? result.get() instanceof InterruptedIOException
                : result.get() instanceof IOException, "Wrong failure: " + result.get());
    }

    public static void main(String[] args) throws Exception {
        AtomicInteger completed = new AtomicInteger();
        DiagnosticRangeExecutor.run(8, lane -> completed.incrementAndGet());
        check(completed.get() == 8, "Not every range lane ran");
        drainTest(false);
        drainTest(true);
        CountDownLatch saturated = new CountDownLatch(DiagnosticRangeExecutor.GLOBAL_LIMIT);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Thread> callers = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            Thread caller = new Thread(() -> {
                try {
                    DiagnosticRangeExecutor.run(4, lane -> {
                        peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                        saturated.countDown();
                        try {
                            await(release);
                        } finally {
                            active.decrementAndGet();
                        }
                    });
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            });
            callers.add(caller);
            caller.start();
        }
        await(saturated);
        check(active.get() == DiagnosticRangeExecutor.GLOBAL_LIMIT, "Did not reach global limit");
        release.countDown();
        for (Thread caller : callers) {
            caller.join(10000);
            check(!caller.isAlive(), "Concurrent caller stuck");
        }
        check(failure.get() == null, "Concurrent read failure: " + failure.get());
        check(active.get() == 0 && peak.get() == DiagnosticRangeExecutor.GLOBAL_LIMIT,
                "Executor-wide bound or drain failed");
        System.out.println("RANGE_EXECUTOR_PROBE_PASS completed=" + completed.get()
                + " peak=" + peak.get() + " failureDrain=true interruptionDrain=true");
    }
}
