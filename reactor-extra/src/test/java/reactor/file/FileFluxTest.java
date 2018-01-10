package reactor.file;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

public class FileFluxTest {

	public static final String FILE_CONTENT =
			"1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n" + "7\n" + "8\n" + "9\n" + "10 11 12";

	@Test
	public void shouldBeAbleToReadFileInFastPath() {
		Path path = Paths.get("./src/test/resources/file.txt");

		Mono<String> fileFlux = FileFlux.from(path)
		                                .reduce(new StringBuffer(),
				                                (sb, bb) -> sb.append(new String(bb.array())))
		                                .map(StringBuffer::toString);

		StepVerifier.create(fileFlux)
		            .expectSubscription()
		            .expectNext(FILE_CONTENT)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void shouldBeAbleToReadFileInSlowPath() {
		Path path = Paths.get("./src/test/resources/file.txt");

		Flux<ByteBuffer> fileFlux = FileFlux.from(path, 8, Schedulers.parallel());

		StepVerifier.create(fileFlux, 1)
		            .expectSubscription()
		            .expectNextCount(1)
		            .thenRequest(1)
		            .expectNextCount(1)
		            .thenRequest(1)
		            .expectNextCount(1)
		            .thenRequest(1)
		            .expectNextCount(1)
		            .thenRequest(1)
		            .expectComplete()
		            .verify();
	}
}
