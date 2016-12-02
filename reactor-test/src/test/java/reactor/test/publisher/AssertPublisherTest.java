package reactor.test.publisher;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.publisher.AssertPublisher.Misbehavior;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Simon Baslé
 */
public class AssertPublisherTest {

	@Test(expected = NullPointerException.class)
	public void normalDisallowsNull() {
		AssertPublisher<String> publisher = AssertPublisher.create();

		publisher.next(null);
	}

	@Test
	public void misbehavingAllowsNull() {
		AssertPublisher<String> publisher = AssertPublisher.createMisbehaving(Misbehavior.ALLOW_NULL);

		StepVerifier.create(publisher)
		            .then(() -> publisher.emit("foo", null))
		            .expectNext("foo", null)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void normalDisallowsOverflow() {
		AssertPublisher<String> publisher = AssertPublisher.create();

		StepVerifier.create(publisher, 1)
		            .then(() -> publisher.next("foo")).as("should pass")
		            .then(() -> publisher.emit("bar")).as("should fail")
		            .expectNext("foo")
		            .expectErrorMatches(e -> e instanceof IllegalStateException &&
		                "Can't deliver value due to lack of requests".equals(e.getMessage()))
		            .verify();

		publisher.assertNoRequestOverflow();
	}

	@Test
	public void misbehavingAllowsOverflow() {
		AssertPublisher<String> publisher = AssertPublisher.createMisbehaving(Misbehavior.REQUEST_OVERFLOW);

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

		publisher.assertRequestOverflow();
	}

	@Test
	public void assertSubscribers() {
		AssertPublisher<String> publisher = AssertPublisher.create();

		try {
			publisher.assertSubscribers();
			fail("expected assertSubscribers to fail");
		} catch (AssertionError e) { }

		StepVerifier.create(publisher)
		            .then(() -> publisher.assertSubscribers()
		                                 .complete())
	                .expectComplete()
	                .verify();
	}

	@Test
	public void assertSubscribersN() {
		AssertPublisher<String> publisher = AssertPublisher.create();

		try {
			publisher.assertSubscribers(1);
			fail("expected assertSubscribers(1) to fail");
		} catch (AssertionError e) { }

		publisher.assertNoSubscribers();
		Flux.from(publisher).subscribe();
		publisher.assertSubscribers(1);
		Flux.from(publisher).subscribe();
		publisher.assertSubscribers(2);

		publisher.complete()
	             .assertNoSubscribers();
	}

	@Test
	public void assertCancelled() {
		AssertPublisher<Object> publisher = AssertPublisher.create();
		StepVerifier.create(publisher)
	                .then(publisher::assertNotCancelled)
	                .thenCancel()
	                .verify();
		publisher.assertCancelled();

		StepVerifier.create(publisher)
	                .then(() -> publisher.assertCancelled(1))
	                .thenCancel()
	                .verify();
		publisher.assertCancelled(2);
	}

}