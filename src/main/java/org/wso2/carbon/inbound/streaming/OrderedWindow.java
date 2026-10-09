/*
 *  Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.carbon.inbound.streaming;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A bounded, in-order execution window: tasks run on up to {@code parallelism} worker threads, but
 * their results are handed back to the submitting thread strictly in submission order.
 * <p>
 * The submitting thread drives everything. {@link #submit} adds a task; once {@code windowSize}
 * tasks are outstanding it blocks on the <em>oldest</em> one and commits it before returning. So:
 * <ul>
 *     <li>at most {@code parallelism} tasks run at once (the worker pool size);</li>
 *     <li>at most {@code windowSize} tasks are held in memory (backpressure on the reader);</li>
 *     <li>commits are always contiguous - when task <i>k</i> is committed, every task before it has
 *     been committed - which is what makes "last committed unit" a safe checkpoint.</li>
 * </ul>
 * A window larger than the pool lets workers keep going while the oldest task is slow; the
 * finished results simply wait their turn to be committed.
 * <p>
 * With {@code parallelism == 1} no thread is created: each task runs inline on the submitting
 * thread and is committed immediately, which is exactly the serial behaviour.
 * <p>
 * Not thread-safe: {@link #submit}, {@link #drain} and {@link #close} must all be called from the
 * one submitting thread. Commits run on that thread too, so committers need no locking.
 *
 * @param <R> the task result type
 */
public final class OrderedWindow<R> implements AutoCloseable {

    /** Receives a task's outcome on the submitting thread, in submission order. */
    public interface Committer<R> {
        /**
         * @param result the task result, or null if the task threw (or was a no-op)
         * @param error  what the task threw, or null on success
         */
        void commit(R result, Throwable error);
    }

    private static final class Entry<R> {
        private final Future<R> future;
        private final Committer<R> committer;

        private Entry(Future<R> future, Committer<R> committer) {
            this.future = future;
            this.committer = committer;
        }
    }

    private final int windowSize;
    private final ExecutorService pool;
    private final Deque<Entry<R>> window = new ArrayDeque<>();

    /**
     * @param parallelism maximum concurrently running tasks; 1 means run inline, no threads
     * @param windowSize  maximum outstanding (uncommitted) tasks; raised to parallelism if lower,
     *                    and ignored (1) when running inline
     * @param threadName  prefix for worker thread names
     */
    public OrderedWindow(int parallelism, int windowSize, String threadName) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be >= 1: " + parallelism);
        }
        // Inline tasks are already finished when submitted; holding one back would only delay its
        // commit (and the checkpoint) behind the next read.
        this.windowSize = parallelism == 1 ? 1 : Math.max(windowSize, parallelism);
        this.pool = parallelism == 1 ? null
                : Executors.newFixedThreadPool(parallelism, new NamedThreadFactory(threadName));
    }

    /**
     * Run {@code task} (or nothing, if it is null) and commit its outcome in submission order. Blocks
     * while the window is full, committing the oldest outstanding task to make room.
     *
     * @throws InterruptedException if interrupted while waiting for the oldest task; that task and
     *                              everything after it stay uncommitted
     */
    public void submit(Callable<R> task, Committer<R> committer) throws InterruptedException {
        window.addLast(new Entry<>(start(task), committer));
        while (window.size() >= windowSize) {
            commitOldest();
        }
    }

    /**
     * Wait for and commit every outstanding task, oldest first.
     *
     * @throws InterruptedException if interrupted while waiting; remaining tasks stay uncommitted
     */
    public void drain() throws InterruptedException {
        while (!window.isEmpty()) {
            commitOldest();
        }
    }

    /** @return the number of submitted tasks not yet committed */
    public int outstanding() {
        return window.size();
    }

    /**
     * Stop the worker pool. Tasks already running are allowed to finish (they are not interrupted:
     * a message half-way through mediation must not be torn down), but nothing further is committed.
     * Call {@link #drain} first on every path that should commit outstanding work.
     */
    @Override
    public void close() {
        window.clear();
        if (pool != null) {
            pool.shutdown();
        }
    }

    private Future<R> start(Callable<R> task) {
        if (task == null) {
            return CompletableFuture.completedFuture(null);
        }
        if (pool != null) {
            return pool.submit(task);
        }
        // Inline: run now, on this thread, and capture the outcome like a pool would.
        CompletableFuture<R> done = new CompletableFuture<>();
        try {
            done.complete(task.call());
        } catch (Throwable t) {
            done.completeExceptionally(t);
        }
        return done;
    }

    private void commitOldest() throws InterruptedException {
        Entry<R> head = window.peekFirst();
        R result = null;
        Throwable error = null;
        try {
            result = head.future.get();
        } catch (ExecutionException e) {
            error = e.getCause() != null ? e.getCause() : e;
        }
        // Remove only once the outcome is known, so an interrupt leaves the head outstanding.
        window.pollFirst();
        head.committer.commit(result, error);
    }

    private static final class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger counter = new AtomicInteger();

        private NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + "-" + counter.incrementAndGet());
            // Never hold the JVM open: on shutdown the streaming loop drains what it needs itself.
            t.setDaemon(true);
            return t;
        }
    }
}
