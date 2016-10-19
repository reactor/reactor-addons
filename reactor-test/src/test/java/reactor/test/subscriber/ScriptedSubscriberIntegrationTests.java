/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.test.subscriber;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static org.junit.Assert.assertEquals;

/**
 * @author Arjen Poutsma
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
public class ScriptedSubscriberIntegrationTests {

	@Test
	public void expectValue() {
		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectValue("bar")
				.expectComplete()
				.verify(() -> Flux.just("foo", "bar"));
	}

	@Test(expected = AssertionError.class)
	public void expectInvalidValue() {
		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectValue("baz")
				.expectComplete()
				.verify(() -> Flux.just("foo", "bar"));
	}

	@Test
	public void expectValueAsync() {
		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectValue("bar")
				.expectComplete()
				.verify(() -> Flux.just("foo", "bar").publishOn(Schedulers.parallel()));
	}

	@Test
	public void expectValues() {
		ScriptedSubscriber.create()
				.expectValues("foo", "bar")
				.expectComplete()
				.verify(() -> Flux.just("foo", "bar"));
	}

	@Test(expected = AssertionError.class)
	public void expectInvalidValues() {
		ScriptedSubscriber.create()
				.expectValues("foo", "baz")
				.expectComplete()
				.verify(() -> Flux.just("foo", "bar"));
	}

	@Test
	public void expectValueWith() {
		ScriptedSubscriber.create()
				.expectValueWith("foo"::equals)
				.expectValueWith("bar"::equals)
				.expectComplete()
				.verify(() -> Flux.just("foo", "bar"));
	}

	@Test(expected = AssertionError.class)
	public void expectInvalidValueWith() {
		ScriptedSubscriber.create()
				.expectValueWith("foo"::equals)
				.expectValueWith("baz"::equals)
				.expectComplete()
				.verify(() -> Flux.just("foo", "bar"));
	}

	@Test
	public void consumeValueWith() throws Exception {
		ScriptedSubscriber<String> subscriber = ScriptedSubscriber.<String>create()
				.consumeValueWith(s -> {
					if (!"foo".equals(s)) {
						throw new AssertionError(s);
					}
				})
				.expectComplete();

		try {
			subscriber.verify(() -> Flux.just("bar"));
		}
		catch (AssertionError error) {
			assertEquals("Expectation failure(s):\n - bar", error.getMessage());
		}
	}

	@Test(expected = AssertionError.class)
	public void missingValue() {
		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectComplete()
				.verify(() -> Flux.just("foo", "bar"));
	}

	@Test(expected = AssertionError.class)
	public void missingValueAsync() {
		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectComplete()
				.verify(() -> Flux.just("foo", "bar")
				                  .publishOn(Schedulers.parallel()));
	}

	@Test
	public void expectValueCount() {
		ScriptedSubscriber.expectValueCount(2)
				.expectComplete()
				.verify(() -> Flux.just("foo", "bar"));
	}

	@Test
	public void error() {
		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectError()
				.verify(() -> Flux.just("foo")
				                  .concatWith(Mono.error(new IllegalArgumentException())));
	}

	@Test
	public void errorClass() {
		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectError(IllegalArgumentException.class)
				.verify(() -> Flux.just("foo")
				                  .concatWith(Mono.error(new IllegalArgumentException())));
	}

	@Test
	public void errorMessage() {
		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectErrorMessage("Error message")
				.verify(() -> Flux.just("foo")
				                  .concatWith(Mono.error(new IllegalArgumentException("Error message"))));
	}

	@Test
	public void errorWith() {
		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectErrorWith(t -> t instanceof IllegalArgumentException)
				.verify(() -> Flux.just("foo")
				                  .concatWith(Mono.error(new IllegalArgumentException())));
	}

	@Test(expected = AssertionError.class)
	public void errorWithInvalid() {
		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectErrorWith(t -> t instanceof IllegalStateException)
				.verify(() -> Flux.just("foo")
				                  .concatWith(Mono.error(new IllegalArgumentException())));
	}

	@Test
	public void consumeErrorWith() {
		try {
			ScriptedSubscriber.create()
					.expectValue("foo")
					.consumeErrorWith(throwable -> {
						if (!(throwable instanceof IllegalStateException)) {
							throw new AssertionError(throwable.getClass().getSimpleName());
						}
					})
					.verify(() -> Flux.just("foo").concatWith(Mono.error(new IllegalArgumentException())));
		}
		catch (AssertionError error) {
			assertEquals("Expectation failure(s):\n - IllegalArgumentException", error.getMessage());
		}
	}

	@Test
	public void request() {
		ScriptedSubscriber.create(1)
				.doRequest(1)
				.expectValue("foo")
				.doRequest(1)
				.expectValue("bar")
				.expectComplete()
				.verify(() -> Flux.just("foo", "bar"));
	}

	@Test
	public void cancel() {
		ScriptedSubscriber.create()
				.expectValue("foo")
				.doCancel()
				.verify(() -> Flux.just("foo", "bar", "baz"));
	}

	@Test(expected = AssertionError.class)
	public void cancelInvalid() {
		ScriptedSubscriber.create()
				.expectValue("foo")
				.doCancel()
				.verify(() -> Flux.just("bar", "baz"));
	}

	@Test(expected = IllegalStateException.class)
	public void notSubscribed() {
		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectComplete()
				.verify(Duration.ofMillis(100));
	}

	@Test
	public void verifyDuration() {
		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectValue("foo")
				.expectComplete()
				.verify(() -> Flux.interval(Duration.ofMillis(200))
				                  .map(l -> "foo")
				                  .take(2),
						Duration.ofMillis(500));
	}

	@Test
	public void verifyVirtualTimeOnSubscribe() {
		ScriptedSubscriber.withVirtualTime()
		                  .advanceTimeBy(Duration.ofDays(3))
		                  .expectValue("foo")
		                  .expectComplete()
		                  .verify(() -> Mono.delay(Duration.ofDays(2))
		                                    .map(l -> "foo"));
	}

	@Test
	public void verifyVirtualTimeOnError() {
		ScriptedSubscriber.withVirtualTime()
		                  .advanceTimeTo(Instant.now().plus(Duration.ofDays(2)))
		                  .expectError(TimeoutException.class)
		                  .verify(() -> Mono.never()
		                                    .timeout(Duration.ofDays(2))
		                                    .map(l -> "foo"));
	}

	@Test
	public void verifyVirtualTimeOnNext() {
		ScriptedSubscriber.withVirtualTime()
		                  .advanceTimeBy(Duration.ofHours(1))
		                  .expectValue("foo")
		                  .advanceTimeBy(Duration.ofHours(1))
		                  .expectValue("bar")
		                  .advanceTimeBy(Duration.ofHours(1))
		                  .expectValue("foobar")
		                  .expectComplete()
		                  .verify(() -> Flux.just("foo", "bar", "foobar")
		                                    .delay(Duration.ofHours(1))
		                                    .log());

	}

	@Test
	public void verifyVirtualTimeOnComplete() {
		ScriptedSubscriber.withVirtualTime()
		                  .advanceTimeBy(Duration.ofHours(1))
		                  .expectComplete()
		                  .verify(() -> Flux.empty()
		                                    .delaySubscription(Duration.ofHours(1))
		                                    .log());
	}

	@Test
	public void verifyVirtualTimeOnNextInterval() {
		ScriptedSubscriber.withVirtualTime()
		                  .advanceTimeBy(Duration.ofSeconds(3))
		                  .expectValue("t0")
		                  .advanceTimeBy(Duration.ofSeconds(3))
		                  .expectValue("t1")
		                  .advanceTimeBy(Duration.ofSeconds(3))
		                  .expectValue("t2")
		                  .doCancel()
		                  .verify(() -> Flux.interval(Duration.ofSeconds(3))
		                                    .map(d -> "t" + d));

	}

	@Test(expected = AssertionError.class)
	public void verifyDurationTimeout() {
		ScriptedSubscriber.create()
				.expectValue("foo")
				.expectValue("foo")
				.expectComplete()
				.verify(() -> Flux.interval(Duration.ofMillis(200))
				                  .map(l -> "foo" )
				                  .take(2),
						Duration.ofMillis(300));
	}

	@Test(expected = NullPointerException.class)
	public void verifyNullSupplier() {
		ScriptedSubscriber.create()
		                  .doCancel()
		                  .verify(() -> null);
	}

	@Test(expected = NullPointerException.class)
	public void verifyDurationNullSupplier() {
		ScriptedSubscriber.create()
		                  .doCancel()
		                  .verify(() -> null, Duration.ofMillis(100));
	}

	@After
	public void cleanup(){
		ScriptedSubscriber.disableVirtualTime();
	}
}