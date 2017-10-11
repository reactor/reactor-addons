package reactor.math

import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux
import java.util.function.Function

//TODO change the doc of sumInt/sumFloat usage if #121 is resolved
//https://github.com/reactor/reactor-addons/issues/121

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
 * Extension to compute the average of values in a [ShortArray] into a [Mono] of [Double].
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
/**
 * Extension to find the highest value in a [ShortArray] and return is as a [Mono]
 * of [Short].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun ShortArray.maxToMono(): Mono<Short> = MathFlux.max(this.toFlux())
/**
 * Extension to find the lowest value in a [ShortArray] and return is as a [Mono]
 * of [Short].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun ShortArray.minToMono(): Mono<Short> = MathFlux.min(this.toFlux())



/**
 * Extension to sum values of a [IntArray] into a [Mono] of [Long].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun IntArray.sumToMono(): Mono<Long> = MathFlux.sumLong(this.toFlux())
/**
 * Extension to sum values of a [IntArray] into a [Mono] of [Int], that
 * wraps around if the sum overflows Int.MAX_VALUE.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
//TODO should cap instead, see
fun IntArray.intSumToMono(): Mono<Int> = MathFlux.sumInt(this.toFlux())
/**
 * Extension to compute the average of values in a [IntArray] into a [Mono] of [Double].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun IntArray.averageToMono(): Mono<Double> = MathFlux.averageDouble(this.toFlux())
/**
 * Extension to compute the average of values in a [IntArray] into a [Mono] of [Float].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun IntArray.floatAverageToMono(): Mono<Float> = MathFlux.averageFloat(this.toFlux())
/**
 * Extension to find the highest value in a [IntArray] and return is as a [Mono]
 * of [Int].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun IntArray.maxToMono(): Mono<Int> = MathFlux.max(this.toFlux())
/**
 * Extension to find the lowest value in a [IntArray] and return is as a [Mono]
 * of [Int].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun IntArray.minToMono(): Mono<Int> = MathFlux.min(this.toFlux())



/**
 * Extension to sum values of a [LongArray] into a [Mono] of [Long].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun LongArray.sumToMono(): Mono<Long> = MathFlux.sumLong(this.toFlux())
/**
 * Extension to sum values of a [LongArray] into a [Mono] of [Int].
 *
 * If one of the values or the sum itself overflows Int.MAX_VALUE, the result will
 * wrap around.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun LongArray.intSumToMono(): Mono<Int> = MathFlux.sumInt(this.toFlux())
/**
 * Extension to compute the average of values in a [LongArray] into a [Mono] of [Double].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun LongArray.averageToMono(): Mono<Double> = MathFlux.averageDouble(this.toFlux())
/**
 * Extension to compute the average of values in a [DoubleArray] into a [Mono] of [Float].
 *
 * If one of the values or the average itself overflows Float.MAX_VALUE, the result will
 * wrap around.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun LongArray.floatAverageToMono(): Mono<Float> = MathFlux.averageFloat(this.toFlux())
/**
 * Extension to find the highest value in a [LongArray] and return is as a [Mono]
 * of [Long].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */fun LongArray.maxToMono(): Mono<Long> = MathFlux.max(this.toFlux())
/**
 * Extension to find the lowest value in a [LongArray] and return is as a [Mono]
 * of [Long].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun LongArray.minToMono(): Mono<Long> = MathFlux.min(this.toFlux())

/**
 * Extension to sum values of a [FloatArray] into a [Mono] of [Double].
 *
 * Note that since Double are more precise, some seemingly rounded Floats (e.g. 1.6f)
 * may convert to Doubles with more decimals (eg. 1.600000023841858), producing sometimes
 * unexpected results.
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
/**
 * Extension to compute the average of values in a [FloatArray] into a [Mono] of [Double].
 *
 * Note that since Double are more precise, some seemingly rounded Floats (e.g. 1.6f)
 * may convert to Doubles with more decimals (eg. 1.600000023841858), producing sometimes
 * unexpected results.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun FloatArray.averageToMono(): Mono<Double> = MathFlux.averageDouble(this.toFlux())
/**
 * Extension to compute the average of values in a [FloatArray] into a [Mono] of [Float].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun FloatArray.floatAverageToMono(): Mono<Float> = MathFlux.averageFloat(this.toFlux())
/**
 * Extension to find the highest value in a [FloatArray] and return is as a [Mono]
 * of [Float].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun FloatArray.maxToMono(): Mono<Float> = MathFlux.max(this.toFlux())
/**
 * Extension to find the lowest value in a [FloatArray] and return is as a [Mono]
 * of [Float].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
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
 *
 * If one of the values or the sum itself overflows Float.MAX_VALUE, the result will
 * cap at [Float.MAX_VALUE].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun DoubleArray.floatSumToMono(): Mono<Float> = MathFlux.sumFloat(this.toFlux())
/**
 * Extension to compute the average of values in a [DoubleArray] into a [Mono] of [Double].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun DoubleArray.averageToMono(): Mono<Double> = MathFlux.averageDouble(this.toFlux())
/**
 * Extension to compute the average of values in a [DoubleArray] into a [Mono] of [Float].
 *
 * If one of the values or the sum itself overflows Float.MAX_VALUE, the result will
 * wrap around.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun DoubleArray.floatAverageToMono(): Mono<Float> = MathFlux.averageFloat(this.toFlux())
/**
 * Extension to find the highest value in a [DoubleArray] and return is as a [Mono]
 * of [Double].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun DoubleArray.maxToMono(): Mono<Double> = MathFlux.max(this.toFlux())
/**
 * Extension to find the lowest value in a [DoubleArray] and return is as a [Mono]
 * of [Double].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun DoubleArray.minToMono(): Mono<Double> = MathFlux.min(this.toFlux())



//collection of numbers
/**
 * Extension to compute the sum of values in a [Collection] of [Number] into a [Mono]
 * of [Long].
 *
 * [Float] and [Double] are rounded to [Long] by [MathFlux], using Java standard
 * conversions.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T: Number> Collection<T>.sumToMono(): Mono<Long> = MathFlux.sumLong(this.toFlux())
/**
 * Extension to compute the sum of values in a [Collection] of [Number] into a [Mono]
 * of [Double], thus avoiding rounding down to zero decimal places.
 *
 * Note that since [Double] are more precise than [Float], some seemingly rounded Floats
 * (e.g. 1.6f) may convert to Doubles with more decimals (eg. 1.600000023841858),
 * producing sometimes unexpected results.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T: Number> Collection<T>.doubleSumToMono(): Mono<Double> = MathFlux.sumDouble(this.toFlux())
/**
 * Extension to compute the average of values in a [Collection] of [Number] into a [Mono]
 * of [Double].
 *
 * Note that since [Double] are more precise than [Float], some seemingly rounded Floats
 * (e.g. 1.6f) may convert to Doubles with more decimals (eg. 1.600000023841858),
 * producing sometimes unexpected results.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T: Number> Collection<T>.averageToMono(): Mono<Double> = MathFlux.averageDouble(this.toFlux())



//collection and arrays of comparables
/**
 * Extension to find the lowest value in a [Collection] and return it as a [Mono].
 * The lowest value is defined by its natural ordering, as represented by the [Comparable]
 * interface.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T: Comparable<T>> Collection<T>.minToMono(): Mono<T> = MathFlux.min(this.toFlux())
/**
 * Extension to find the highest value in a [Collection] and return it as a [Mono].
 * The highest value is defined by its natural ordering, as represented by the [Comparable]
 * interface.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T: Comparable<T>> Collection<T>.maxToMono(): Mono<T> = MathFlux.max(this.toFlux())
/**
 * Extension to find the lowest value in an [Array] and return it as a [Mono].
 * The lowest value is defined by its natural ordering, as represented by the [Comparable]
 * interface.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T: Comparable<T>> Array<out T>.minToMono(): Mono<T> = MathFlux.min(this.toFlux())
/**
 * Extension to find the highest value in an [Array] and return it as a [Mono].
 * The highest value is defined by its natural ordering, as represented by the [Comparable]
 * interface.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T: Comparable<T>> Array<out T>.maxToMono(): Mono<T> = MathFlux.max(this.toFlux())



//collection and arrays with comparator
/**
 * Extension to find the lowest value in a [Collection] and return it as a [Mono].
 * The lowest value is defined by comparisons made using a provided [Comparator].
 *
 * @param comp The [Comparator] to use
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Collection<T>.minToMono(comp: Comparator<T>): Mono<T> = MathFlux.min(this.toFlux(), comp)
/**
 * Extension to find the highest value in a [Collection] and return it as a [Mono].
 * The highest value is defined by comparisons made using a provided [Comparator].
 *
 * @param comp The [Comparator] to use
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Collection<T>.maxToMono(comp: Comparator<T>): Mono<T> = MathFlux.max(this.toFlux(), comp)
/**
 * Extension to find the lowest value in an [Array] and return it as a [Mono].
 * The lowest value is defined by comparisons made using a provided [Comparator].
 *
 * @param comp The [Comparator] to use
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Array<out T>.minToMono(comp: Comparator<T>): Mono<T> = MathFlux.min(this.toFlux(), comp)
/**
 * Extension to find the highest value in an [Array] and return it as a [Mono].
 * The highest value is defined by comparisons made using a provided [Comparator].
 *
 * @param comp The [Comparator] to use
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Array<out T>.maxToMono(comp: Comparator<T>): Mono<T> = MathFlux.max(this.toFlux(), comp)



//collection with mapping to Number
/**
 * Extension to map arbitrary values in a [Collection] to [Number]s and return
 * the sum of these Numbers as a [Mono] of [Long].
 *
 * [Float] and [Double] are rounded to [Long] by [MathFlux], using Java standard
 * conversions.
 *
 * @param mapper a lambda converting values to [Number]
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Collection<T>.mapSumToMono(mapper: (T) -> Number): Mono<Long>
    = MathFlux.sumLong(this.toFlux(), Function(mapper))
/**
 * Extension to map arbitrary values in a [Collection] to [Number]s and return
 * the sum of these Numbers as a [Mono] of [Double], thus avoiding rounding down to
 * zero decimal places.
 *
 * Note that since [Double] are more precise than [Float], some seemingly rounded Floats
 * (e.g. 1.6f) may convert to Doubles with more decimals (eg. 1.600000023841858),
 * producing sometimes unexpected results.
 *
 * @param mapper a lambda converting values to [Number]
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Collection<T>.mapDoubleSumToMono(mapper: (T) -> Number): Mono<Double>
    = MathFlux.sumDouble(this.toFlux(), Function(mapper))
/**
 * Extension to map arbitrary values in a [Collection] to [Number]s and return
 * the average of these Numbers as a [Mono] of [Double].
 *
 * Note that since [Double] are more precise than [Float], some seemingly rounded Floats
 * (e.g. 1.6f) may convert to Doubles with more decimals (eg. 1.600000023841858),
 * producing sometimes unexpected results.
 *
 * @param mapper a lambda converting values to [Number]
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Collection<T>.mapAverageToMono(mapper: (T) -> Number): Mono<Double>
    = MathFlux.averageDouble(this.toFlux(), Function(mapper))