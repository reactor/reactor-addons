/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.adapter.rxjava;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.Test;

import io.reactivex.*;
import io.reactivex.schedulers.Schedulers;

public class RxJava2SchedulersTest {

    @SuppressWarnings("unchecked")
    static void runRxJavaTest(boolean expected, String name, Supplier<Scheduler> supplier) {
        Flowable.fromCallable(() -> Thread.currentThread().getName().contains(name))
        .subscribeOn(supplier.get())
        .test()
        .awaitDone(5, TimeUnit.SECONDS)
        .assertResult(expected);

        Single.fromCallable(() -> Thread.currentThread().getName().contains(name))
        .subscribeOn(supplier.get())
        .test()
        .awaitDone(5, TimeUnit.SECONDS)
        .assertResult(expected);

        Flowable.timer(100, TimeUnit.MILLISECONDS, supplier.get())
        .map(v -> Thread.currentThread().getName().contains(name))
        .test()
        .awaitDone(5, TimeUnit.SECONDS)
        .assertResult(expected);

        Flowable.fromCallable(() -> Thread.currentThread().getName().contains(name))
        .subscribeOn(supplier.get())
        .delay(20, TimeUnit.MILLISECONDS, Schedulers.computation())
        .test()
        .awaitDone(5, TimeUnit.SECONDS)
        .assertResult(expected);

        Flowable.interval(20, TimeUnit.MILLISECONDS, supplier.get())
        .map(v -> Thread.currentThread().getName().contains(name))
        .take(5)
        .test()
        .awaitDone(5, TimeUnit.SECONDS)
        .assertResult(expected, expected, expected, expected, expected);

        Flowable.fromCallable(() -> Thread.currentThread().getName().contains(name))
        .subscribeOn(supplier.get())
        .buffer(10, 20, TimeUnit.MILLISECONDS, supplier.get())
        .test()
        .awaitDone(5, TimeUnit.SECONDS)
        .assertResult(Collections.singletonList(expected));
    }
    
    @Test
    public void useAndThenDisable() {
        
        // -----------------------------------------------------------------------------------------
        
        runRxJavaTest(true, "RxComputation", () -> Schedulers.computation());
        runRxJavaTest(true, "RxSingle", () -> Schedulers.single());
        runRxJavaTest(true, "RxCached", () -> Schedulers.io());
        runRxJavaTest(true, "RxNewThread", () -> Schedulers.newThread());

        // -----------------------------------------------------------------------------------------

        RxJava2Schedulers.useReactorCoreSchedulers();

        runRxJavaTest(false, "RxComputation", () -> Schedulers.computation());
        runRxJavaTest(false, "RxSingle", () -> Schedulers.single());
        runRxJavaTest(false, "RxCached", () -> Schedulers.io());
        runRxJavaTest(false, "RxNewThread", () -> Schedulers.newThread());

        // -----------------------------------------------------------------------------------------

        RxJava2Schedulers.resetReactorCoreSchedulers();

        runRxJavaTest(true, "RxComputation", () -> Schedulers.computation());
        runRxJavaTest(true, "RxSingle", () -> Schedulers.single());
        runRxJavaTest(true, "RxCached", () -> Schedulers.io());
        runRxJavaTest(true, "RxNewThread", () -> Schedulers.newThread());
    }
}
