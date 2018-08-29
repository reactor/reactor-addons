package reactor.bool

import org.junit.Test
import reactor.core.publisher.Mono
import reactor.test.test

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
    fun booleanAnd() {
        booleanTable(Mono<Boolean>::andBoolean,
                true, false,
                false, false)
    }

    @Test
    fun booleanOr() {
        booleanTable(Mono<Boolean>::orBoolean,
                true, true,
                true, false)
    }

    @Test
    fun booleanNand() {
        booleanTable(Mono<Boolean>::nand,
                false, true,
                true, true)
    }

    @Test
    fun booleanNor() {
        booleanTable(Mono<Boolean>::nor,
                false, false,
                false, true)
    }

    @Test
    fun booleanXor() {
        booleanTable(Mono<Boolean>::xor,
                false, true,
                true, false)
    }

    @Test
    fun booleanNot() {
        mTrue.not().test().expectNext(false).`as`("not(true)").verifyComplete()

        mFalse.not().test().expectNext(true).`as`("not(false)").verifyComplete()
    }

    @Test
    fun booleanNotOperator() {
        (!mTrue).test().expectNext(false).`as`("!true").verifyComplete()

        (!mFalse).test().expectNext(true).`as`("!false").verifyComplete()
    }
}