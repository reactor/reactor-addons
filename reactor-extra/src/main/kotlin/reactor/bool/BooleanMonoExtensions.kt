package reactor.bool

import reactor.core.publisher.Mono

/**
 * Extension to logically revert a [Boolean] [Mono]. It can also be
 * applied as a ! operator.
 *
 * @author Simon Baslé
 * @since 3.2.0
 */
@Deprecated("To be removed in 3.3.0.RELEASE, replaced by module reactor-kotlin-extensions",
        ReplaceWith("not()", "reactor.kotlin.extra.bool.not"))
operator fun Mono<Boolean>.not(): Mono<Boolean> = BooleanUtils.not(this)

/**
 * Extension to logically combine two [Boolean] [Mono] with the AND operator.
 *
 * @author Simon Baslé
 * @since 3.2.0
 */
@Deprecated("To be removed in 3.3.0.RELEASE, replaced by module reactor-kotlin-extensions",
        ReplaceWith("logicalAnd(rightHand)", "reactor.kotlin.extra.bool.logicalHand"))
fun Mono<Boolean>.logicalAnd(rightHand: Mono<Boolean>): Mono<Boolean> = BooleanUtils.and(this, rightHand)

/**
 * Extension to logically combine two [Boolean] [Mono] with the Not-AND (NAND) operator.
 *
 * @author Simon Baslé
 * @since 3.2.0
 */
@Deprecated("To be removed in 3.3.0.RELEASE, replaced by module reactor-kotlin-extensions",
        ReplaceWith("logicalNAnd(rightHand)", "reactor.kotlin.extra.bool.logicalNAnd"))
fun Mono<Boolean>.logicalNAnd(rightHand: Mono<Boolean>): Mono<Boolean> = BooleanUtils.nand(this, rightHand)

/**
 * Extension to logically combine two [Boolean] [Mono] with the OR operator.
 *
 * @author Simon Baslé
 * @since 3.2.0
 */
@Deprecated("To be removed in 3.3.0.RELEASE, replaced by module reactor-kotlin-extensions",
        ReplaceWith("logicalOr()", "reactor.kotlin.extra.bool.logicalOr"))
fun Mono<Boolean>.logicalOr(rightHand: Mono<Boolean>): Mono<Boolean> = BooleanUtils.or(this, rightHand)

/**
 * Extension to logically combine two [Boolean] [Mono] with the Not-OR (NOR) operator.
 *
 * @author Simon Baslé
 * @since 3.2.0
 */
@Deprecated("To be removed in 3.3.0.RELEASE, replaced by module reactor-kotlin-extensions",
        ReplaceWith("logicalNOr()", "reactor.kotlin.extra.bool.logicalNOr"))
fun Mono<Boolean>.logicalNOr(rightHand: Mono<Boolean>): Mono<Boolean> = BooleanUtils.nor(this, rightHand)

/**
 * Extension to logically combine two [Boolean] [Mono] with the exclusive-OR (XOR) operator.
 *
 * @author Simon Baslé
 * @since 3.2.0
 */
@Deprecated("To be removed in 3.3.0.RELEASE, replaced by module reactor-kotlin-extensions",
        ReplaceWith("logicalXOr()", "reactor.kotlin.extra.bool.logicalXOr"))
fun Mono<Boolean>.logicalXOr(rightHand: Mono<Boolean>): Mono<Boolean> = BooleanUtils.xor(this, rightHand)