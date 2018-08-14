package reactor.bool;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.bool.BooleanUtils.*;
import static reactor.bool.BooleanUtils.not;

import org.junit.Test;
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
			.assertNext(bool -> assertThat(bool).as("not(TRUE)").isFalse())
			.verifyComplete();
		
		StepVerifier.create(not(FALSE))
			.assertNext(bool -> assertThat(bool).as("not(FALSE)").isTrue())
			.verifyComplete();
	}

	@Test
	public void testAnd() {
		StepVerifier.create(and(TRUE, TRUE))
			.assertNext(bool -> assertThat(bool).as("and(TRUE, TRUE)").isTrue())
			.verifyComplete();

		StepVerifier.create(and(TRUE, FALSE))
			.assertNext(bool -> assertThat(bool).as("and(TRUE, FALSE)").isFalse())
			.verifyComplete();

		StepVerifier.create(and(FALSE, TRUE))
			.assertNext(bool -> assertThat(bool).as("and(FALSE, TRUE)").isFalse())
			.verifyComplete();

		StepVerifier.create(and(FALSE, FALSE))
			.assertNext(bool -> assertThat(bool).as("and(FALSE, FALSE)").isFalse())
			.verifyComplete();
	}

	@Test
	public void testOr() {
		StepVerifier.create(or(TRUE, TRUE))
			.assertNext(bool -> assertThat(bool).as("or(TRUE, TRUE)").isTrue())
			.verifyComplete();

		StepVerifier.create(or(TRUE, FALSE))
			.assertNext(bool -> assertThat(bool).as("or(TRUE, FALSE)").isTrue())
			.verifyComplete();

		StepVerifier.create(or(FALSE, TRUE))
			.assertNext(bool -> assertThat(bool).as("or(FALSE, TRUE)").isTrue())
			.verifyComplete();

		StepVerifier.create(or(FALSE, FALSE))
			.assertNext(bool -> assertThat(bool).as("or(FALSE, FALSE)").isFalse())
			.verifyComplete();
	}

	@Test
	public void testNand() {
		StepVerifier.create(nand(TRUE, TRUE))
			.assertNext(bool -> assertThat(bool).as("nand(TRUE, TRUE)").isFalse())
			.verifyComplete();

		StepVerifier.create(nand(TRUE, FALSE))
			.assertNext(bool -> assertThat(bool).as("nand(TRUE, FALSE)").isTrue())
			.verifyComplete();

		StepVerifier.create(nand(FALSE, TRUE))
			.assertNext(bool -> assertThat(bool).as("nand(FALSE, TRUE)").isTrue())
			.verifyComplete();

		StepVerifier.create(nand(FALSE, FALSE))
			.assertNext(bool -> assertThat(bool).as("nand(FALSE, FALSE)").isTrue())
			.verifyComplete();
	}

	@Test
	public void testNor() {
		StepVerifier.create(nor(TRUE, TRUE))
			.assertNext(bool -> assertThat(bool).as("nor(TRUE, TRUE)").isFalse())
			.verifyComplete();

		StepVerifier.create(nor(TRUE, FALSE))
			.assertNext(bool -> assertThat(bool).as("nor(TRUE, FALSE)").isFalse())
			.verifyComplete();

		StepVerifier.create(nor(FALSE, TRUE))
			.assertNext(bool -> assertThat(bool).as("nor(FALSE, TRUE)").isFalse())
			.verifyComplete();

		StepVerifier.create(nor(FALSE, FALSE))
			.assertNext(bool -> assertThat(bool).as("nor(FALSE, FALSE)").isTrue())
			.verifyComplete();
	}

	@Test
	public void testXor() {
		StepVerifier.create(xor(TRUE, TRUE))
			.assertNext(bool -> assertThat(bool).as("xor(TRUE, TRUE)").isFalse())
			.verifyComplete();

		StepVerifier.create(xor(TRUE, FALSE))
			.assertNext(bool -> assertThat(bool).as("xor(TRUE, FALSE)").isTrue())
			.verifyComplete();

		StepVerifier.create(xor(FALSE, TRUE))
			.assertNext(bool -> assertThat(bool).as("xor(FALSE, TRUE)").isTrue())
			.verifyComplete();

		StepVerifier.create(xor(FALSE, FALSE))
			.assertNext(bool -> assertThat(bool).as("xor(FALSE, FALSE)").isFalse())
			.verifyComplete();
	}
}
