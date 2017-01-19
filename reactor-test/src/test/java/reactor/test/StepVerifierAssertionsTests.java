package reactor.test;

import java.time.Duration;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class StepVerifierAssertionsTests {

	@Test
	public void assertDroppedElementsAllPass() {
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onNext("foo");
			s.onComplete();
			s.onNext("bar");
			s.onNext("baz");
		}).take(3))
		            .expectNext("foo")
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDroppedElements()
		            .hasDropped("baz")
		            .hasDroppedExactly("baz", "bar");
	}

	@Test
	public void assertDroppedElementsFailureNoDrop() {
		try {
			StepVerifier.create(Mono.empty())
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedElements();
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected dropped elements, none found.");
		}
	}

	@Test
	public void assertDroppedElementsFailureOneExtra() {
		try {
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onNext("foo");
				s.onComplete();
				s.onNext("bar");
				s.onNext("baz");
			}).take(3))
			            .expectNext("foo")
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDropped("foo");
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected dropped elements to contain <[foo]>, was <[bar, baz]>.");
		}
	}

	@Test
	public void assertDroppedElementsFailureOneMissing() {
		try {
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onNext("foo");
				s.onComplete();
				s.onNext("bar");
				s.onNext("baz");
			}).take(3))
			            .expectNext("foo")
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedExactly("baz");
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected dropped elements to contain exactly <[baz]>, was <[bar, baz]>.");
		}
	}

	@Test
	public void assertDroppedErrorAllPass() {
		Throwable err1 = new IllegalStateException("boom1");
		Throwable err2 = new IllegalStateException("boom2");
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onError(err1);
			s.onError(err2);
		}).buffer(1))
		            .expectError()
		            .verifyThenAssertThat()
		            .hasDroppedError()
		            .hasDroppedErrorOfType(IllegalStateException.class)
		            .hasDroppedErrorWithMessageContaining("boom")
		            .hasDroppedErrorWithMessage("boom2")
		            .hasDroppedErrorMatching(t -> t instanceof IllegalStateException && "boom2".equals(t.getMessage()));
	}

	@Test
	public void assertDroppedErrorFailureNoDrop() {
		try {
			StepVerifier.create(Mono.empty())
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedError();
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected dropped error, none found.");
		}
	}

	@Test
	public void assertDroppedErrorFailureWrongType() {
		try {
			Throwable err1 = new IllegalStateException("boom1");
			Throwable err2 = new IllegalStateException("boom2");
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onError(err1);
				s.onError(err2);
			}).buffer(1))
			            .expectError()
			            .verifyThenAssertThat()
			            .hasDroppedErrorOfType(IllegalArgumentException.class);
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected dropped error to be of type java.lang.IllegalArgumentException, was java.lang.IllegalStateException.");
		}
	}

	@Test
	public void assertDroppedErrorFailureWrongContains() {
		try {
			Throwable err1 = new IllegalStateException("boom1");
			Throwable err2 = new IllegalStateException("boom2");
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onError(err1);
				s.onError(err2);
			}).buffer(1))
			            .expectError()
			            .verifyThenAssertThat()
			            .hasDroppedErrorWithMessageContaining("foo");
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected dropped error with message containing <\"foo\">, was <\"boom2\">.");
		}
	}

	@Test
	public void assertDroppedErrorFailureWrongMessage() {
		try {
			Throwable err1 = new IllegalStateException("boom1");
			Throwable err2 = new IllegalStateException("boom2");
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onError(err1);
				s.onError(err2);
			}).buffer(1))
			            .expectError()
			            .verifyThenAssertThat()
			            .hasDroppedErrorWithMessage("boom1");
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected dropped error with message <\"boom1\">, was <\"boom2\">.");
		}
	}

	@Test
	public void assertDroppedErrorFailureWrongMatch() {
		try {
			Throwable err1 = new IllegalStateException("boom1");
			Throwable err2 = new IllegalStateException("boom2");
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onError(err1);
				s.onError(err2);
			}).buffer(1))
			            .expectError()
			            .verifyThenAssertThat()
			            .hasDroppedErrorMatching(t -> t instanceof IllegalStateException && "foo".equals(t.getMessage()));
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected dropped error matching the given predicate, did not match: <java.lang.IllegalStateException: boom2>.");
		}
	}

	@Test
	public void assertDurationLessThanOk() {
		StepVerifier.create(Mono.delay(Duration.ofMillis(500)).then())
		            .expectComplete()
		            .verifyThenAssertThat()
		            .tookLessThan(Duration.ofSeconds(1));
	}

	@Test
	public void assertDurationLessThanFailure() {
		try {
			StepVerifier.create(Mono.delay(Duration.ofMillis(500)).then())
		                .expectComplete()
		                .verifyThenAssertThat()
		                .tookLessThan(Duration.ofMillis(200));

			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae)
					//the actual duration can vary a bit
					.hasMessageStartingWith("Expected scenario to be verified in less than 200ms, took 5")
					.hasMessageEndingWith("ms.");
		}
	}

	@Test
	public void assertDurationConsidersEqualsASuccess() {
		new DefaultStepVerifierBuilder.DefaultStepVerifierAssertions(null, null, Duration.ofSeconds(3))
				.tookLessThan(Duration.ofMillis(3000L))
				.tookMoreThan(Duration.ofSeconds(3));
	}

	@Test
	public void assertDurationMoreThanOk() {
		StepVerifier.create(Mono.delay(Duration.ofMillis(500)).then())
		            .expectComplete()
		            .verifyThenAssertThat()
		            .tookMoreThan(Duration.ofMillis(100));
	}

	@Test
	public void assertDurationMoreThanFailure() {
		try {
			StepVerifier.create(Mono.delay(Duration.ofMillis(500)).then())
		                .expectComplete()
		                .verifyThenAssertThat()
		                .tookMoreThan(Duration.ofMillis(800));

			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae)
					//the actual duration can vary a bit
					.hasMessageStartingWith("Expected scenario to be verified in more than 800ms, took 5")
					.hasMessageEndingWith("ms.");
		}
	}

}
