package reactor.retry;

import java.time.Duration;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BackoffTest {

	@Test
	public void toStringZero() {
		assertThat(Backoff.zero().toString()).isEqualTo("Backoff{ZERO}");
	}

	@Test
	public void toStringFixed() {
		assertThat(Backoff.fixed(Duration.ofMillis(123)).toString())
				.isEqualTo("Backoff{fixed=123ms}");
	}

	@Test
	public void toStringExponentialWithMax() {
		assertThat(Backoff.exponential(Duration.ofMillis(1), Duration.ofMillis(123),
				8, false).toString())
				.isEqualTo("Backoff{exponential,min=1ms,max=123ms,factor=8,basedOnPreviousValue=false}");
	}

	@Test
	public void toStringExponentialNoMax() {
		assertThat(Backoff.exponential(Duration.ofMillis(1), null,
				8, false).toString())
				.isEqualTo("Backoff{exponential,min=1ms,max=NONE,factor=8,basedOnPreviousValue=false}");
	}

	@Test
	public void toStringExponentialWithMaxDependsPrevious() {
		assertThat(Backoff.exponential(Duration.ofMillis(1), Duration.ofMillis(123),
				8, true).toString())
				.isEqualTo("Backoff{exponential,min=1ms,max=123ms,factor=8,basedOnPreviousValue=true}");
	}

	@Test
	public void toStringExponentialNoMaxDependsPrevious() {
		assertThat(Backoff.exponential(Duration.ofMillis(1), null,
				8, true).toString())
				.isEqualTo("Backoff{exponential,min=1ms,max=NONE,factor=8,basedOnPreviousValue=true}");
	}

}