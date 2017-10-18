package reactor.math

import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.function.Function


/**
 * Extension to compute the [Long] sum of all values emitted by a [Publisher] of [Number]
 * (typically a [Flux]) and return it as a [Mono] of [Long].
 *
 * Note that summing decimal numbers with this method loses precision, see [sumDouble].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T: Number> Publisher<T>.sum(): Mono<Long> = MathFlux.sumLong(this)

/**
 * Extension to compute the [Double] sum of all values emitted by a [Publisher] of
 * [Number] (typically a [Flux]) and return it as a [Mono] of [Double].
 *
 * Note that since Double are more precise, some seemingly rounded Floats (e.g. 1.6f)
 * may convert to Doubles with more decimals (eg. 1.600000023841858), producing sometimes
 * unexpected sums.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T: Number> Publisher<T>.sumDouble(): Mono<Double> = MathFlux.sumDouble(this)
/**
 * Extension to compute the [Double] average of all values emitted by a [Publisher] of
 * [Number] (typically a [Flux]) and return it as a [Mono] of [Double].
 *
 * Note that since Double are more precise, some seemingly rounded Floats (e.g. 1.6f)
 * may convert to Doubles with more decimals (eg. 1.600000023841858), producing sometimes
 * unexpected averages.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T: Number> Publisher<T>.average(): Mono<Double> = MathFlux.averageDouble(this)

//min and max that work on any comparable
/**
 * Extension to find the lowest value in a [Publisher] (typically a [Flux]) of [Comparable]
 * values and return it as a [Mono] of [T].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T: Comparable<T>> Publisher<T>.min(): Mono<T> = MathFlux.min(this)
/**
 * Extension to find the highest value in a [Publisher] (typically a [Flux]) of [Comparable]
 * values and return it as a [Mono] of [T].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T: Comparable<T>> Publisher<T>.max(): Mono<T> = MathFlux.max(this)

//sum/sumDouble/average lambda versions where a converter is provided
/**
 * Extension to map arbitrary values in a [Publisher] (typically a [Flux]) to [Number]s
 * and return the sum of these Numbers as a [Mono] of [Long].
 *
 * [Float] and [Double] are rounded to [Long] by [MathFlux], using Java standard
 * conversions.
 *
 * @param mapper a lambda converting values to [Number]
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Publisher<T>.sum(mapper: (T) -> Number): Mono<Long>
        = MathFlux.sumLong(this, Function(mapper))
/**
 * Extension to map arbitrary values in a [Publisher] (typically a [Flux]) to [Number]s
 * and return the sum of these Numbers as a [Mono] of [Double], thus avoiding rounding
 * down to zero decimal places.
 *
 * Note that since [Double] are more precise than [Float], some seemingly rounded Floats
 * (e.g. 1.6f) may convert to Doubles with more decimals (eg. 1.600000023841858),
 * producing sometimes unexpected results.
 *
 * @param mapper a lambda converting values to [Number]
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Publisher<T>.sumDouble(mapper: (T) -> Number): Mono<Double>
        = MathFlux.sumDouble(this, Function(mapper))
/**
 * Extension to map arbitrary values in a [Publisher] (typically a [Flux]) to [Number]s
 * and return the average of these Numbers as a [Mono] of [Double].
 *
 * Note that since [Double] are more precise than [Float], some seemingly rounded Floats
 * (e.g. 1.6f) may convert to Doubles with more decimals (eg. 1.600000023841858),
 * producing sometimes unexpected results.
 *
 * @param mapper a lambda converting values to [Number]
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Publisher<T>.average(mapper: (T) -> Number): Mono<Double>
        = MathFlux.averageDouble(this, Function(mapper))


//min/max lambda versions where a comparator or equivalent function is provided
/**
 * Extension to find the lowest value in a [Publisher] (typically a [Flux]) and return it
 * as a [Mono].The lowest value is defined by comparisons made using a provided [Comparator].
 *
 * @param comp The [Comparator] to use
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Publisher<T>.min(comp: Comparator<T>): Mono<T> = MathFlux.min(this, comp)
/**
 * Extension to find the lowest value in a [Publisher] (typically a [Flux]) and return it
 * as a [Mono].The lowest value is defined by comparisons made using a provided function
 * that behaves like a [Comparator].
 *
 * @param comp The comparison function to use (similar to a [Comparator])
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Publisher<T>.min(comp: (T, T) -> Int): Mono<T> = MathFlux.min(this, Comparator(comp))
/**
 * Extension to find the highest value in a [Publisher] (typically a [Flux]) and return it
 * as a [Mono].The highest value is defined by comparisons made using a provided
 * [Comparator].
 *
 * @param comp The [Comparator] to use
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Publisher<T>.max(comp: Comparator<T>): Mono<T> = MathFlux.max(this, comp)
/**
 * Extension to find the highest value in a [Publisher] (typically a [Flux]) and return it
 * as a [Mono].The highest value is defined by comparisons made using a provided function
 * that behaves like a [Comparator].
 *
 * @param comp The comparison function to use (similar to a [Comparator])
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Publisher<T>.max(comp: (T, T) -> Int): Mono<T> = MathFlux.max(this, Comparator(comp))