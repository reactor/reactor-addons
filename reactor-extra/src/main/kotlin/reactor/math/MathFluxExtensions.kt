package reactor.math

import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux
import java.util.function.Function

/**
 * Extension to sum values of a [ShortArray] into a [Mono] of [Long].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun ShortArray.sumToMono(): Mono<Long> = MathFlux.sumLong(this.toFlux())
/**
 * Extension to sum values of a [ShortArray] into a [Mono] of [Int], that
 * wraps around if the sum overflows Int.MAX_VALUE.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun ShortArray.intSumToMono(): Mono<Int> = MathFlux.sumInt(this.toFlux())
/**
 * Extension to compute the average of values in a [ShortArray] into a [Mono] of [Long].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun ShortArray.averageToMono(): Mono<Double> = MathFlux.averageDouble(this.toFlux())
/**
 * Extension to compute the average of values in a [ShortArray] into a [Mono] of [Float].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun ShortArray.floatAverageToMono(): Mono<Float> = MathFlux.averageFloat(this.toFlux())
fun ShortArray.maxToMono(): Mono<Short> = MathFlux.max(this.toFlux())
fun ShortArray.minToMono(): Mono<Short> = MathFlux.min(this.toFlux())

/**
 * Extension to sum values of a [IntArray] into a [Mono] of [Long].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun IntArray.sumToMono(): Mono<Long> = MathFlux.sumLong(this.toFlux())
fun IntArray.intSumToMono(): Mono<Int> = MathFlux.sumInt(this.toFlux())
fun IntArray.averageToMono(): Mono<Double> = MathFlux.averageDouble(this.toFlux())
fun IntArray.floatAverageToMono(): Mono<Float> = MathFlux.averageFloat(this.toFlux())
fun IntArray.maxToMono(): Mono<Int> = MathFlux.max(this.toFlux())
fun IntArray.minToMono(): Mono<Int> = MathFlux.min(this.toFlux())

/**
 * Extension to sum values of a [LongArray] into a [Mono] of [Long].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun LongArray.sumToMono(): Mono<Long> = MathFlux.sumLong(this.toFlux())
fun LongArray.intSumToMono(): Mono<Int> = MathFlux.sumInt(this.toFlux())
fun LongArray.averageToMono(): Mono<Double> = MathFlux.averageDouble(this.toFlux())
fun LongArray.floatAverageToMono(): Mono<Float> = MathFlux.averageFloat(this.toFlux())
fun LongArray.maxToMono(): Mono<Long> = MathFlux.max(this.toFlux())
fun LongArray.minToMono(): Mono<Long> = MathFlux.min(this.toFlux())

/**
 * Extension to sum values of a [FloatArray] into a [Mono] of [Double].
 * <p>
 * Note that since Double are more precise, some seemingly round Float (e.g. 1.6f)
 * may convert to unrounded doubles.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun FloatArray.sumToMono(): Mono<Double> = MathFlux.sumDouble(this.toFlux())
/**
 * Extension to sum values of a [FloatArray] into a [Mono] of [Float].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun FloatArray.floatSumToMono(): Mono<Float> = MathFlux.sumFloat(this.toFlux())
fun FloatArray.averageToMono(): Mono<Double> = MathFlux.averageDouble(this.toFlux())
fun FloatArray.floatAverageToMono(): Mono<Float> = MathFlux.averageFloat(this.toFlux())
fun FloatArray.maxToMono(): Mono<Float> = MathFlux.max(this.toFlux())
fun FloatArray.minToMono(): Mono<Float> = MathFlux.min(this.toFlux())

/**
 * Extension to sum values of a [DoubleArray] into a [Mono] of [Double].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun DoubleArray.sumToMono(): Mono<Double> = MathFlux.sumDouble(this.toFlux())
/**
 * Extension to sum values of a [DoubleArray] into a [Mono] of [Float].
 * <p>
 * Note that since Double are more precise, some seemingly round Float (e.g. 1.6f)
 * may convert to unrounded doubles.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun DoubleArray.floatSumToMono(): Mono<Float> = MathFlux.sumFloat(this.toFlux())
fun DoubleArray.averageToMono(): Mono<Double> = MathFlux.averageDouble(this.toFlux())
fun DoubleArray.floatAverageToMono(): Mono<Float> = MathFlux.averageFloat(this.toFlux())
fun DoubleArray.maxToMono(): Mono<Double> = MathFlux.max(this.toFlux())
fun DoubleArray.minToMono(): Mono<Double> = MathFlux.min(this.toFlux())

//collection of numbers
fun <T: Number> Collection<T>.sumToMono(): Mono<Long> = MathFlux.sumLong(this.toFlux())
fun <T: Number> Collection<T>.doubleSumToMono(): Mono<Double> = MathFlux.sumDouble(this.toFlux())
fun <T: Number> Collection<T>.averageToMono(): Mono<Double> = MathFlux.averageDouble(this.toFlux())

//collection and arrays of comparables
fun <T: Comparable<T>> Collection<T>.minToMono(): Mono<T> = MathFlux.min(this.toFlux())
fun <T: Comparable<T>> Collection<T>.maxToMono(): Mono<T> = MathFlux.max(this.toFlux())
fun <T: Comparable<T>> Array<out T>.minToMono(): Mono<T> = MathFlux.min(this.toFlux())
fun <T: Comparable<T>> Array<out T>.maxToMono(): Mono<T> = MathFlux.max(this.toFlux())

//collection and arrays with comparator
fun <T> Collection<T>.minToMono(comp: Comparator<T>): Mono<T> = MathFlux.min(this.toFlux(), comp)
fun <T> Collection<T>.maxToMono(comp: Comparator<T>): Mono<T> = MathFlux.max(this.toFlux(), comp)
fun <T> Array<out T>.minToMono(comp: Comparator<T>): Mono<T> = MathFlux.min(this.toFlux(), comp)
fun <T> Array<out T>.maxToMono(comp: Comparator<T>): Mono<T> = MathFlux.max(this.toFlux(), comp)

//collection with mapping to Number

fun <T> Collection<T>.mapSumToMono(mapper: (T) -> Number): Mono<Long>
    = MathFlux.sumLong(this.toFlux(), Function(mapper))
fun <T> Collection<T>.mapDoubleSumToMono(mapper: (T) -> Number): Mono<Double>
    = MathFlux.sumDouble(this.toFlux(), Function(mapper))
fun <T> Collection<T>.mapAverageToMono(mapper: (T) -> Number): Mono<Double>
    = MathFlux.averageDouble(this.toFlux(), Function(mapper))
