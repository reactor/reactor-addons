package reactor.retry;

import java.time.Duration;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultRetryTest {

	@Test
	public void stringJustOnce() {
		Retry test = Retry.any().retryOnce();

		assertThat(test.toString())
				.isEqualTo("Retry{max=1,backoff=Backoff{ZERO},jitter=Jitter{NONE}}");
	}

	@Test
	public void stringTwiceFixedNoJitter() {
		Retry test = Retry.any()
		                  .retryMax(2)
		                  .backoff(Backoff.fixed(Duration.ofHours(2)));

		assertThat(test.toString())
				.isEqualTo("Retry{max=2,backoff=Backoff{fixed=7200000ms},jitter=Jitter{NONE}}");
	}

	@Test
	public void stringThreeTimesExponentialRandomJitter() {
		Backoff backoff = Backoff.exponential(
				Duration.ofMillis(12),
				Duration.ofMinutes(2),
				3,
				true);
		Retry test = Retry.any()
		                  .retryMax(3)
		                  .backoff(backoff)
		                  .jitter(Jitter.random());

		assertThat(test.toString())
				.isEqualTo("Retry{max=3,backoff=" + backoff + ",jitter=Jitter{RANDOM-0.5}}");
	}

	//TODO change in 3.2.0 to "doesnt change maxIterations" when gh-149 and gh-150 are fixed
	@Test
	public void timeoutDoesChangeMaxIterations() {
		final DefaultRetry<Object> retry1 = (DefaultRetry<Object>) Retry.any()
		                                                                .retryMax(3);

		assertThat(retry1.maxIterations).isEqualTo(3);

		final DefaultRetry<Object> retry2 =
				(DefaultRetry<Object>) retry1.timeout(Duration.ofMillis(200));

		assertThat(retry2.maxIterations).as("switched to unlimited by timeout")
		                                .isEqualTo(Integer.MAX_VALUE);

		final DefaultRetry<Object> retry3 =
				(DefaultRetry<Object>) retry2.retryMax(4);

		assertThat(retry3.maxIterations).as("back to limited by retryMax")
		                                .isEqualTo(4);
	}

}