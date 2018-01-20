package reactor.file;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public abstract class FileFlux extends Flux<ByteBuffer> {

	static final int DEFAULT_BUFFER_CAPACITY = 1024;

	public static FileFlux from(Path path) {
		return new FileChannelReaderFlux(path, DEFAULT_BUFFER_CAPACITY, Schedulers.parallel());
	}

	public static FileFlux from(Path path, int bufferCapacity, Scheduler scheduler) {
		return new FileChannelReaderFlux(path, bufferCapacity, scheduler);
	}

	public Flux<ByteBuffer> lines() {
		return null;
	}

	public Mono<Void> save(Path path) {
return null;
	}
}
