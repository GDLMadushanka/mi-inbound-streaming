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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class OrderedWindowTest {

    @Test
    public void commitsInSubmissionOrderDespiteRandomCompletion() throws Exception {
        List<Integer> committed = new ArrayList<>();
        Random random = new Random(42);
        try (OrderedWindow<Integer> window = new OrderedWindow<>(8, 16, "test")) {
            for (int i = 0; i < 200; i++) {
                int n = i;
                int sleep = random.nextInt(5);
                window.submit(() -> {
                    Thread.sleep(sleep);
                    return n;
                }, (result, error) -> committed.add(result));
            }
            window.drain();
        }
        assertEquals(200, committed.size());
        for (int i = 0; i < 200; i++) {
            assertEquals(Integer.valueOf(i), committed.get(i));
        }
    }

    @Test
    public void neverRunsMoreThanParallelismNorHoldsMoreThanTheWindow() throws Exception {
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maxRunning = new AtomicInteger();
        int maxOutstanding = 0;
        try (OrderedWindow<Void> window = new OrderedWindow<>(4, 8, "test")) {
            for (int i = 0; i < 100; i++) {
                window.submit(() -> {
                    maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
                    Thread.sleep(2);
                    running.decrementAndGet();
                    return null;
                }, (result, error) -> { });
                maxOutstanding = Math.max(maxOutstanding, window.outstanding());
            }
            window.drain();
        }
        assertTrue("ran " + maxRunning.get() + " at once", maxRunning.get() <= 4);
        assertTrue("expected real concurrency, got " + maxRunning.get(), maxRunning.get() > 1);
        assertTrue("held " + maxOutstanding + " outstanding", maxOutstanding < 8);
    }

    @Test
    public void workersKeepGoingWhileTheOldestTaskIsSlow() throws Exception {
        // Task 0 blocks until every later task in the window has finished: possible only if the
        // window lets later tasks run (and complete) while the oldest one is outstanding.
        int parallelism = 2;
        int windowSize = 4;
        CountDownLatch othersDone = new CountDownLatch(windowSize - 1);
        List<Integer> committed = new ArrayList<>();
        try (OrderedWindow<Integer> window = new OrderedWindow<>(parallelism, windowSize, "test")) {
            window.submit(() -> {
                assertTrue("later tasks never ran", othersDone.await(5, TimeUnit.SECONDS));
                return 0;
            }, (result, error) -> committed.add(result));
            for (int i = 1; i < windowSize; i++) {
                int n = i;
                window.submit(() -> {
                    othersDone.countDown();
                    return n;
                }, (result, error) -> committed.add(result));
            }
            window.drain();
        }
        assertEquals(4, committed.size());
        assertEquals(Integer.valueOf(0), committed.get(0));
        assertEquals(Integer.valueOf(3), committed.get(3));
    }

    @Test
    public void parallelismOneRunsInlineAndCommitsImmediately() throws Exception {
        Thread caller = Thread.currentThread();
        List<Thread> ranOn = new ArrayList<>();
        List<Integer> committed = new ArrayList<>();
        try (OrderedWindow<Integer> window = new OrderedWindow<>(1, 2, "test")) {
            for (int i = 0; i < 3; i++) {
                int n = i;
                window.submit(() -> {
                    ranOn.add(Thread.currentThread());
                    return n;
                }, (result, error) -> committed.add(result));
                // Serial behaviour: each unit is committed before the next one is read.
                assertEquals(0, window.outstanding());
                assertEquals(n + 1, committed.size());
            }
        }
        for (Thread t : ranOn) {
            assertSame(caller, t);
        }
    }

    @Test
    public void taskFailureIsReportedToItsCommitterWithoutStoppingTheOthers() throws Exception {
        List<String> outcomes = new ArrayList<>();
        try (OrderedWindow<String> window = new OrderedWindow<>(3, 6, "test")) {
            for (int i = 0; i < 5; i++) {
                int n = i;
                window.submit(() -> {
                    if (n == 2) {
                        throw new IllegalStateException("boom");
                    }
                    return "ok" + n;
                }, (result, error) -> outcomes.add(error != null ? error.getMessage() : result));
            }
            window.drain();
        }
        assertEquals(5, outcomes.size());
        assertEquals("ok1", outcomes.get(1));
        assertEquals("boom", outcomes.get(2));
        assertEquals("ok4", outcomes.get(4));
    }

    @Test
    public void nullTaskIsCommittedInItsPlace() throws Exception {
        List<String> committed = new ArrayList<>();
        try (OrderedWindow<String> window = new OrderedWindow<>(2, 4, "test")) {
            window.submit(() -> {
                Thread.sleep(20);
                return "slow";
            }, (result, error) -> committed.add(result));
            window.submit(null, (result, error) -> {
                assertNull(result);
                committed.add("skipped");
            });
            window.drain();
        }
        assertEquals("slow", committed.get(0));
        assertEquals("skipped", committed.get(1));
    }
}
