package reactor.retry;

import org.junit.Test;
import org.junit.runner.RunWith;

import javax.naming.OperationNotSupportedException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.*;

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

	//TODO 4 tests below have very similar structure and could benefit from JUnitParams
	@Test
	public void exponentialDoesntThrowArithmeticException_explicitMax() {
		final Duration EXPLICIT_MAX = Duration.ofSeconds(100_000);
		final Duration INIT = Duration.ofSeconds(10);

		Backoff backoff = Backoff.exponential(INIT, EXPLICIT_MAX, 2, false);

		BackoffDelay delay = null;
		Context<String> context = null;
		for (int i = 0; i < 71; i++) {
			if (i == 0) {
				delay = new BackoffDelay(INIT, EXPLICIT_MAX, INIT);
			}
			else {
				context = new DefaultContext<>(null, i, delay, null);
				delay = backoff.apply(context);
			}
		}

		assertThat(context).isNotNull();
		assertThat(delay.delay).isEqualTo(EXPLICIT_MAX);
		assertThat(context.iteration()).isEqualTo(70);
		assertThat(context.backoff()).isEqualTo(EXPLICIT_MAX);
	}

	@Test
	public void exponentialDoesntThrowArithmeticException_noSpecificMax() {
		final Duration INIT = Duration.ofSeconds(10);
		final Duration EXPECTED_MAX = Duration.ofSeconds(Long.MAX_VALUE);

		Backoff backoff = Backoff.exponential(INIT, null, 2, false);

		BackoffDelay delay = null;
		Context<String> context = null;
		for (int i = 0; i < 71; i++) {
			if (i == 0) {
				delay = new BackoffDelay(INIT, null, INIT);
			}
			else {
				context = new DefaultContext<>(null, i, delay, null);
				delay = backoff.apply(context);
			}
		}

		assertThat(context).isNotNull();
		assertThat(delay.delay).isEqualTo(EXPECTED_MAX);
		assertThat(context.iteration()).isEqualTo(70);
		assertThat(context.backoff()).isEqualTo(EXPECTED_MAX);
	}

	@Test
	public void exponentialDoesntThrowArithmeticException_explicitMaxDependsOnPrevious() {
		final Duration EXPLICIT_MAX = Duration.ofSeconds(100_000);
		final Duration INIT = Duration.ofSeconds(10);

		Backoff backoff = Backoff.exponential(INIT, EXPLICIT_MAX, 2, true);

		BackoffDelay delay = null;
		Context<String> context = null;
		for (int i = 0; i < 71; i++) {
			if (i == 0) {
				delay = new BackoffDelay(INIT, EXPLICIT_MAX, INIT);
			}
			else {
				context = new DefaultContext<>(null, i, delay, null);
				delay = backoff.apply(context);
			}
		}

		assertThat(context).isNotNull();
		assertThat(delay.delay).isEqualTo(EXPLICIT_MAX);
		assertThat(context.iteration()).isEqualTo(70);
		assertThat(context.backoff()).isEqualTo(EXPLICIT_MAX);
	}

	@Test
	public void exponentialDoesntThrowArithmeticException_noSpecificMaxDependsOnPrevious() {
		final Duration INIT = Duration.ofSeconds(10);
		final Duration EXPECTED_MAX = Duration.ofSeconds(Long.MAX_VALUE);

		Backoff backoff = Backoff.exponential(INIT, null, 2, true);

		BackoffDelay delay = null;
		Context<String> context = null;
		for (int i = 0; i < 71; i++) {
			if (i == 0) {
				delay = new BackoffDelay(INIT, null, INIT);
			}
			else {
				context = new DefaultContext<>(null, i, delay, null);
				delay = backoff.apply(context);
			}
		}

		assertThat(context).isNotNull();
		assertThat(delay.delay).isEqualTo(EXPECTED_MAX);
		assertThat(context.iteration()).isEqualTo(70);
		assertThat(context.backoff()).isEqualTo(EXPECTED_MAX);
	}

	@Test
	public void exponentialRejectsMaxLowerThanFirst() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> Backoff.exponential(Duration.ofSeconds(2), Duration.ofSeconds(1), 1, false))
				.as("not based on previous value")
				.withMessage("maxBackoff must be >= firstBackoff");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> Backoff.exponential(Duration.ofSeconds(2), Duration.ofSeconds(1), 1, true))
				.as("based on previous value")
				.withMessage("maxBackoff must be >= firstBackoff");
	}

	@Test
	public void exponentialAcceptsMaxEqualToFirst() {
		assertThatCode(() -> Backoff.exponential(Duration.ofSeconds(1), Duration.ofSeconds(1), 1, false))
				.as("not based on previous value")
				.doesNotThrowAnyException();

		assertThatCode(() -> Backoff.exponential(Duration.ofSeconds(1), Duration.ofSeconds(1), 1, true))
				.as("based on previous value")
				.doesNotThrowAnyException();
	}

}