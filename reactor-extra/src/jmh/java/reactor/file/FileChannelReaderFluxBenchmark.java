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
