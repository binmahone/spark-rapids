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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/** Dedicated bounded workers for the local-file range-concurrency experiment. */
final class DiagnosticRangeExecutor {
    static final int GLOBAL_LIMIT = 32;
    private static final Semaphore PERMITS = new Semaphore(GLOBAL_LIMIT, true);
    private static final AtomicInteger THREAD_IDS = new AtomicInteger();
    private static final AtomicInteger ACTIVE = new AtomicInteger();
    private static final AtomicInteger PEAK = new AtomicInteger();
    private static final ExecutorService POOL = Executors.newFixedThreadPool(GLOBAL_LIMIT, task -> {
        Thread thread = new Thread(task, "paul-range-io-" + THREAD_IDS.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    });

    @FunctionalInterface
    interface LaneRead {
        void read(int lane) throws IOException;
    }

    /** All submitted workers finish before return or throw; the caller owns their buffers. */
    static int run(int lanes, LaneRead read) throws IOException {
        if (lanes < 1 || lanes > GLOBAL_LIMIT) {
            throw new IllegalArgumentException("Invalid diagnostic lane count");
        }
        List<FutureTask<Void>> futures = new ArrayList<>(lanes);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        Throwable failure = null;
        boolean interrupted = false;
        try {
            for (int lane = 0; lane < lanes; lane++) {
                final int index = lane;
                FutureTask<Void> task = new FutureTask<>(() -> {
                    peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                    PEAK.accumulateAndGet(ACTIVE.incrementAndGet(), Math::max);
                    try {
                        read.read(index);
                        return null;
                    } finally {
                        ACTIVE.decrementAndGet();
                        active.decrementAndGet();
                        PERMITS.release();
                    }
                });
                PERMITS.acquire();
                try {
                    POOL.execute(task);
                } catch (RuntimeException | Error e) {
                    PERMITS.release();
                    throw e;
                }
                futures.add(task);
            }
        } catch (InterruptedException e) {
            interrupted = true;
            failure = new InterruptedIOException("Interrupted submitting diagnostic range reads");
        } catch (RuntimeException | Error e) {
            failure = e;
        }
        for (FutureTask<Void> task : futures) {
            boolean done = false;
            while (!done) {
                try {
                    task.get();
                    done = true;
                } catch (InterruptedException e) {
                    interrupted = true;
                    if (failure == null) {
                        failure = new InterruptedIOException("Interrupted waiting for range reads");
                    }
                } catch (ExecutionException e) {
                    done = true;
                    if (failure == null) {
                        failure = e.getCause();
                    } else {
                        failure.addSuppressed(e.getCause());
                    }
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        if (failure instanceof IOException) {
            throw (IOException) failure;
        } else if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        } else if (failure instanceof Error) {
            throw (Error) failure;
        } else if (failure != null) {
            throw new IOException(failure);
        }
        return peak.get();
    }

    static int globalPeak() {
        return PEAK.get();
    }

    private DiagnosticRangeExecutor() {
    }
}
