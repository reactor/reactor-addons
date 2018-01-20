package reactor.file;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import reactor.core.publisher.Operators;

public class AsyncFileChannelReaderFluxBenchmark {
	@Benchmark()
	@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
	public void smallFileLowChanksSizeMAXDemand() {
		Path path = Paths.get("./src/jmh/resources/file.txt");
		new AsyncFileChannelReaderFlux(path, 1).blockLast();
	}

	@Benchmark()
	@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
	public void smallFileDefaultChanksSizeMAXDemand() {
		Path path = Paths.get("./src/jmh/resources/file.txt");
		new AsyncFileChannelReaderFlux(path, 1024).blockLast();
	}


	@Benchmark()
	@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
	public void largeFileDefaultChanksSizeMAXDemand() {
		Path path = Paths.get("./src/jmh/resources/shakespeare.txt");
		new AsyncFileChannelReaderFlux(path, 1024).blockLast();
	}

	@Benchmark()
	@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
	public void largeFileDefaultChanksSizeIterativeDemand() {
		Path path = Paths.get("./src/jmh/resources/shakespeare.txt");
		TestBlockingLastSubscriber<ByteBuffer> subscriber = new
				TestBlockingLastSubscriber<>();
		new AsyncFileChannelReaderFlux(path,
				1024).subscribe(Operators.toCoreSubscriber(subscriber));

		subscriber.blockingGet();
	}
}
