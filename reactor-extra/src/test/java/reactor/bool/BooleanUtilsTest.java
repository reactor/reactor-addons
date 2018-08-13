package reactor.bool;

import static org.assertj.core.api.Assertions.*;
import static reactor.bool.BooleanUtils.*;
import static reactor.bool.BooleanUtils.not;

import org.testng.annotations.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * @author Greg Turnquist
 */
public class BooleanUtilsTest {

	private static final Mono<Boolean> TRUE = Mono.just(true);
	private static final Mono<Boolean> FALSE = Mono.just(false);

	@Test
	public void testNot() {
		StepVerifier.create(not(TRUE))
			.assertNext(bool -> assertThat(bool).isFalse())
			.verifyComplete();
		
		StepVerifier.create(not(FALSE))
			.assertNext(bool -> assertThat(bool).isTrue())
			.verifyComplete();
	}

	@Test
	public void testAnd() {
		StepVerifier.create(and(TRUE, TRUE))
			.assertNext(bool -> assertThat(bool).isTrue())
			.verifyComplete();

		StepVerifier.create(and(TRUE, FALSE))
			.assertNext(bool -> assertThat(bool).isFalse())
			.verifyComplete();

		StepVerifier.create(and(FALSE, TRUE))
			.assertNext(bool -> assertThat(bool).isFalse())
			.verifyComplete();

		StepVerifier.create(and(FALSE, FALSE))
			.assertNext(bool -> assertThat(bool).isFalse())
			.verifyComplete();
	}

	@Test
	public void testOr() {
		StepVerifier.create(or(TRUE, TRUE))
			.assertNext(bool -> assertThat(bool).isTrue())
			.verifyComplete();

		StepVerifier.create(or(TRUE, FALSE))
			.assertNext(bool -> assertThat(bool).isTrue())
			.verifyComplete();

		StepVerifier.create(or(FALSE, TRUE))
			.assertNext(bool -> assertThat(bool).isTrue())
			.verifyComplete();

		StepVerifier.create(or(FALSE, FALSE))
			.assertNext(bool -> assertThat(bool).isFalse())
			.verifyComplete();
	}

	@Test
	public void testNand() {
		StepVerifier.create(nand(TRUE, TRUE))
			.assertNext(bool -> assertThat(bool).isFalse())
			.verifyComplete();

		StepVerifier.create(nand(TRUE, FALSE))
			.assertNext(bool -> assertThat(bool).isTrue())
			.verifyComplete();

		StepVerifier.create(nand(FALSE, TRUE))
			.assertNext(bool -> assertThat(bool).isTrue())
			.verifyComplete();

		StepVerifier.create(nand(FALSE, FALSE))
			.assertNext(bool -> assertThat(bool).isTrue())
			.verifyComplete();
	}

	@Test
	public void testNor() {
		StepVerifier.create(nor(TRUE, TRUE))
			.assertNext(bool -> assertThat(bool).isFalse())
			.verifyComplete();

		StepVerifier.create(nor(TRUE, FALSE))
			.assertNext(bool -> assertThat(bool).isFalse())
			.verifyComplete();

		StepVerifier.create(nor(FALSE, TRUE))
			.assertNext(bool -> assertThat(bool).isFalse())
			.verifyComplete();

		StepVerifier.create(nor(FALSE, FALSE))
			.assertNext(bool -> assertThat(bool).isTrue())
			.verifyComplete();
	}

	@Test
	public void testXor() {
		StepVerifier.create(xor(TRUE, TRUE))
			.assertNext(bool -> assertThat(bool).isFalse())
			.verifyComplete();

		StepVerifier.create(xor(TRUE, FALSE))
			.assertNext(bool -> assertThat(bool).isTrue())
			.verifyComplete();

		StepVerifier.create(xor(FALSE, TRUE))
			.assertNext(bool -> assertThat(bool).isTrue())
			.verifyComplete();

		StepVerifier.create(xor(FALSE, FALSE))
			.assertNext(bool -> assertThat(bool).isFalse())
			.verifyComplete();
	}
}