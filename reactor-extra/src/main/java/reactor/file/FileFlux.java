package reactor.file;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class FileFlux {

	static final int DEFAULT_BUFFER_CAPACITY = 1024;

	public static Flux<ByteBuffer> from(Path path) {
		return new FileReaderFlux(path, DEFAULT_BUFFER_CAPACITY, Schedulers.parallel());
	}

	public static Flux<ByteBuffer> from(Path path, int bufferCapacity, Scheduler scheduler) {
		return new FileReaderFlux(path, bufferCapacity, scheduler);
	}
}
