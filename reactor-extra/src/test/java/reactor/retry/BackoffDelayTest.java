package reactor.retry;

import java.time.Duration;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
public class BackoffDelayTest {

	@Test
	public void toStringZero() {
		assertThat(BackoffDelay.ZERO.toString())
				.isEqualTo("{ZERO}");
	}

	@Test
	public void toStringExhausted() {
		assertThat(AbstractRetry.RETRY_EXHAUSTED.toString())
				.isEqualTo("{EXHAUSTED}");
	}

	@Test
	public void toStringSimple() {
		assertThat(new BackoffDelay(Duration.ofSeconds(3)).toString())
				.isEqualTo("{3000ms}");
	}

	@Test
	public void toStringMinMaxDelay() {
		assertThat(new BackoffDelay(
				Duration.ofSeconds(3),
				Duration.ofSeconds(4),
				Duration.ofMillis(123)
		).toString())
				.isEqualTo("{123ms/4000ms}");
	}

}