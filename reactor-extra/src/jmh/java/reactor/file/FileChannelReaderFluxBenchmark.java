/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.file;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ForkJoinPool;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.scheduler.forkjoin.ForkJoinPoolScheduler;

public class FileChannelReaderFluxBenchmark {

	public static final Scheduler SCHEDULER = Schedulers.fromExecutor(ForkJoinPool.commonPool());

	@Benchmark()
	@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
	public void smallFileLowChanksSizeMAXDemand() {
		Path path = Paths.get("./src/jmh/resources/file.txt");
		new FileChannelReaderFlux(path, 1, SCHEDULER).blockLast();
	}

	@Benchmark()
	@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
	public void smallFileDefaultChanksSizeMAXDemand() {
		Path path = Paths.get("./src/jmh/resources/file.txt");
		new FileChannelReaderFlux(path, 1024, SCHEDULER).blockLast();
	}

	@Benchmark()
	@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
	public void largeFileDefaultChanksSizeMAXDemand() {
		Path path = Paths.get("./src/jmh/resources/shakespeare.txt");
		new FileChannelReaderFlux(path, 1024, SCHEDULER).blockLast();
	}

	@Benchmark()
	@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
	public void largeFileDefaultChanksSizeIterativeDemand() {
		Path path = Paths.get("./src/jmh/resources/shakespeare.txt");
		TestBlockingLastSubscriber<ByteBuffer> subscriber = new
				TestBlockingLastSubscriber<>();
		new FileChannelReaderFlux(path,
				1024, SCHEDULER).subscribe(Operators.toCoreSubscriber(subscriber));

		subscriber.blockingGet();
	}
}
