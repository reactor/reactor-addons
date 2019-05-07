package reactor.bool

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import reactor.core.publisher.Mono
import reactor.kotlin.test.test

class BooleanMonoExtensionsTests {

    private val mTrue: Mono<Boolean> = Mono.just(true)
    private val mFalse: Mono<Boolean> = Mono.just(false)

    private fun booleanTable(
            op: (Mono<Boolean>, Mono<Boolean>) -> Mono<Boolean>,
            expectedTrueTrue: Boolean,
            expectedTrueFalse: Boolean,
            expectedFalseTrue: Boolean,
            expectedFalseFalse: Boolean) {

        op.invoke(mTrue, mTrue).test()
                .expectNext(expectedTrueTrue)
                .`as`("(true, true)")
                .verifyComplete()
        op.invoke(mTrue, mFalse).test()
                .expectNext(expectedTrueFalse)
                .`as`("(true, false)")
                .verifyComplete()
        op.invoke(mFalse, mTrue).test()
                .expectNext(expectedFalseTrue)
                .`as`("(false, true)")
                .verifyComplete()
        op.invoke(mFalse, mFalse).test()
                .expectNext(expectedFalseFalse)
                .`as`("(false, false)")
                .verifyComplete()
    }

    @Test
    fun smokeTest() {
        assertThat(mTrue.logicalAnd(mFalse).block()).`as`("true AND false").isFalse()
        assertThat(mTrue.logicalOr(mFalse).block()).`as`("true OR false").isTrue()
        assertThat(mTrue.logicalNAnd(mFalse).block()).`as`("true NAND false").isTrue()
        assertThat(mTrue.logicalNOr(mFalse).block()).`as`("true NOR false").isFalse()
        assertThat(mTrue.logicalXOr(mFalse).block()).`as`("true XOR false").isTrue()
    }

    @Test
    fun logicalAnd() {
        booleanTable(Mono<Boolean>::logicalAnd,
                true, false,
                false, false)
    }

    @Test
    fun logicalOr() {
        booleanTable(Mono<Boolean>::logicalOr,
                true, true,
                true, false)
    }

    @Test
    fun logicalNand() {
        booleanTable(Mono<Boolean>::logicalNAnd,
                false, true,
                true, true)
    }

    @Test
    fun logicalNor() {
        booleanTable(Mono<Boolean>::logicalNOr,
                false, false,
                false, true)
    }

    @Test
    fun logicalXor() {
        booleanTable(Mono<Boolean>::logicalXOr,
                false, true,
                true, false)
    }

    @Test
    fun not() {
        mTrue.not().test().expectNext(false).`as`("not(true)").verifyComplete()

        mFalse.not().test().expectNext(true).`as`("not(false)").verifyComplete()
    }

    @Test
    fun notWithOperator() {
        (!mTrue).test().expectNext(false).`as`("!true").verifyComplete()

        (!mFalse).test().expectNext(true).`as`("!false").verifyComplete()
    }
}