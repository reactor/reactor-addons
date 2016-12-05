package reactor.test.publisher;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.ValidatingPublisher.Misbehavior;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Simon Baslé
 */
public class ValidatingPublisherTest {

	@Test(expected = NullPointerException.class)
	public void normalDisallowsNull() {
		ValidatingPublisher<String> publisher = ValidatingPublisher.create();

		publisher.next(null);
	}

	@Test
	public void misbehavingAllowsNull() {
		ValidatingPublisher<String> publisher = ValidatingPublisher.createMisbehaving(Misbehavior.ALLOW_NULL);

		StepVerifier.create(publisher)
		            .then(() -> publisher.emit("foo", null))
		            .expectNext("foo", null)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void normalDisallowsOverflow() {
		ValidatingPublisher<String> publisher = ValidatingPublisher.create();

		StepVerifier.create(publisher, 1)
		            .then(() -> publisher.next("foo")).as("should pass")
		            .then(() -> publisher.emit("bar")).as("should fail")
		            .expectNext("foo")
		            .expectErrorMatches(e -> e instanceof IllegalStateException &&
		                "Can't deliver value due to lack of requests".equals(e.getMessage()))
		            .verify();

		publisher.expectNoRequestOverflow();
	}

	@Test
	public void misbehavingAllowsOverflow() {
		ValidatingPublisher<String> publisher = ValidatingPublisher.createMisbehaving(Misbehavior.REQUEST_OVERFLOW);

		try {
			StepVerifier.create(publisher, 1)
			            .then(() -> publisher.emit("foo", "bar"))
			            .expectNext("foo")
			            .expectComplete() //n/a
			            .verify();
			fail();
		}
		catch (AssertionError e) {
			assertThat(e.getMessage(), containsString("expected production of at most 1;"));
		}

		publisher.expectRequestOverflow();
	}

	@Test
	public void expectSubscribers() {
		ValidatingPublisher<String> publisher = ValidatingPublisher.create();

		try {
			publisher.expectSubscribers();
			fail("expected expectSubscribers to fail");
		} catch (AssertionError e) { }

		StepVerifier.create(publisher)
		            .then(() -> publisher.expectSubscribers()
		                                 .complete())
	                .expectComplete()
	                .verify();
	}

	@Test
	public void expectSubscribersN() {
		ValidatingPublisher<String> publisher = ValidatingPublisher.create();

		try {
			publisher.expectSubscribers(1);
			fail("expected expectSubscribers(1) to fail");
		} catch (AssertionError e) { }

		publisher.expectNoSubscribers();
		Flux.from(publisher).subscribe();
		publisher.expectSubscribers(1);
		Flux.from(publisher).subscribe();
		publisher.expectSubscribers(2);

		publisher.complete()
	             .expectNoSubscribers();
	}

	@Test
	public void expectCancelled() {
		ValidatingPublisher<Object> publisher = ValidatingPublisher.create();
		StepVerifier.create(publisher)
	                .then(publisher::expectNotCancelled)
	                .thenCancel()
	                .verify();
		publisher.expectCancelled();

		StepVerifier.create(publisher)
	                .then(() -> publisher.expectCancelled(1))
	                .thenCancel()
	                .verify();
		publisher.expectCancelled(2);
	}

	@Test
	public void expectMinRequestedNormal() {
		ValidatingPublisher<String> publisher = ValidatingPublisher.create();

		StepVerifier.create(Flux.from(publisher).limitRate(5))
	                .then(publisher::expectNotCancelled)
	                .then(() -> publisher.expectMinRequested(5))
	                .thenCancel()
	                .verify();
		publisher.expectCancelled();
		publisher.expectNoSubscribers();
		publisher.expectMinRequested(0);
	}

	@Test
	public void expectMinRequestedFailure() {
		ValidatingPublisher<String> publisher = ValidatingPublisher.create();

		try {

		StepVerifier.create(Flux.from(publisher).limitRate(5))
		            .then(() -> publisher.expectMinRequested(6)
		                                 .emit("foo"))
		            .expectNext("foo").expectComplete() // N/A
		            .verify();
			fail("expected expectMinRequested(6) to fail");
		}
		catch (AssertionError e) {
			assertThat(e.getMessage(), containsString("Expected minimum request of 6; got 5"));
		}

		publisher.expectCancelled();
		publisher.expectNoSubscribers();
		publisher.expectMinRequested(0);
	}

}