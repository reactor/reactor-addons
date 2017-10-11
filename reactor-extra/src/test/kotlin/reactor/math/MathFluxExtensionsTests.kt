package reactor.math

import org.junit.Test
import reactor.core.publisher.Flux
import reactor.test.test

/**
 * @author Simon Baslé
 */
class MathFluxExtensionsTests {

    data class User(val age: Int,val name: String)

    companion object {
        val userList = listOf(User(18, "bob"),
                User(80, "grandpa"),
                User(1, "baby"))

        val userArray = arrayOf(User(18, "bob"),
                User(80, "grandpa"),
                User(1, "baby"))

        val comparator: Comparator<User> = Comparator({ u1: User, u2:User -> u1.age - u2.age})

        val comparableList = listOf("AA", "A", "BB", "B", "AB")
        val comparableArray = arrayOf("AA", "A", "BB", "B", "AB")
    }

//== ShortArray ==

    @Test
    fun shortArraySum() {
        shortArrayOf(32_000, 8_000) //sum overflows a Short
                .sumToMono()
                .test()
                .expectNext(40_000)
                .verifyComplete()
    }

    @Test
    fun shortArrayIntSum() {
        shortArrayOf(32_000, 8_000) //sum overflows a Short
                .intSumToMono()
                .test()
                .expectNext(40_000)
                .verifyComplete()
    }

    @Test
    fun shortArrayAverage() {
        shortArrayOf(10, 11)
                .averageToMono()
                .test()
                .expectNext(10.5)
                .verifyComplete()
    }

    @Test
    fun shortArrayAverageFloat() {
        shortArrayOf(10, 11)
                .floatAverageToMono()
                .test()
                .expectNext(10.5f)
                .verifyComplete()
    }

    @Test
    fun shortArrayMin() {
        shortArrayOf(12, 8, 16)
                .minToMono()
                .test()
                .expectNext(8)
                .verifyComplete()
    }

    @Test
    fun shortArrayMax() {
        shortArrayOf(12, 16, 8)
                .maxToMono()
                .test()
                .expectNext(16)
                .verifyComplete()
    }

//== IntArray ==

    @Test
    fun intArraySum() {
        intArrayOf(2_000_000_000, 200_000_000) //sum overflows an Int
                .sumToMono()
                .test()
                .expectNext(2_200_000_000)
                .verifyComplete()
    }

    @Test
    fun intArrayIntSumWrapsAround() {
        intArrayOf(Int.MAX_VALUE, 1) //sum overflows an Int
                .intSumToMono()
                .test()
                .expectNext(Int.MIN_VALUE)
                .verifyComplete()
    }

    @Test
    fun intArrayAverage() {
        intArrayOf(10, 11)
                .averageToMono()
                .test()
                .expectNext(10.5)
                .verifyComplete()
    }

    @Test
    fun intArrayAverageFloat() {
        intArrayOf(10, 11)
                .floatAverageToMono()
                .test()
                .expectNext(10.5f)
                .verifyComplete()
    }

    @Test
    fun intArrayMin() {
        intArrayOf(12, 8, Int.MIN_VALUE, 16)
                .minToMono()
                .test()
                .expectNext(Int.MIN_VALUE)
                .verifyComplete()
    }

    @Test
    fun intArrayMax() {
        intArrayOf(12, Int.MAX_VALUE, 16, 8)
                .maxToMono()
                .test()
                .expectNext(Int.MAX_VALUE)
                .verifyComplete()
    }

//== LongArray ==

    @Test
    fun longArraySum() {
        longArrayOf(3_000_000_000, 2_000_000_000)
                .sumToMono()
                .test()
                .expectNext(5_000_000_000)
                .verifyComplete()
    }

    @Test
    fun longArrayIntSumWrapsAround() {
        longArrayOf(Int.MAX_VALUE.toLong(), 1L)
                .intSumToMono()
                .test()
                .expectNext(Int.MIN_VALUE)
                .verifyComplete()
    }

    @Test
    fun longArrayAverage() {
        longArrayOf(10L, 11L)
                .averageToMono()
                .test()
                .expectNext(10.5)
                .verifyComplete()
    }

    @Test
    fun longAverageFloat() {
        longArrayOf(10L, 11L)
                .floatAverageToMono()
                .test()
                .expectNext(10.5f)
                .verifyComplete()
    }

    @Test
    fun longArrayMin() {
        longArrayOf(12, 8, Long.MIN_VALUE, 16)
                .minToMono()
                .test()
                .expectNext(Long.MIN_VALUE)
                .verifyComplete()
    }

    @Test
    fun longArrayMax() {
        longArrayOf(12, Long.MAX_VALUE, 16, 8)
                .maxToMono()
                .test()
                .expectNext(Long.MAX_VALUE)
                .verifyComplete()
    }

//== FloatArray ==

    @Test
    fun floatArraySum() {
        floatArrayOf(3.5f, 1.5f)
                .sumToMono()
                .test()
                .expectNext(5.0)
                .verifyComplete()
    }

    @Test
    fun floatArrayFloatSumCaps() {
        floatArrayOf(Float.MAX_VALUE, 1.0f)
                .floatSumToMono()
                .test()
                .expectNext(Float.MAX_VALUE)
                .verifyComplete()
    }

    @Test
    fun floatArrayAverage() {
        floatArrayOf(10f, 11f)
                .averageToMono()
                .test()
                .expectNext(10.5)
                .verifyComplete()
    }

    @Test
    fun floatAverageFloat() {
        floatArrayOf(10f, 11f)
                .floatAverageToMono()
                .test()
                .expectNext(10.5f)
                .verifyComplete()
    }

    @Test
    fun floatArrayMin() {
        floatArrayOf(12.3f, 15f, 12.2f, 12.4f)
                .minToMono()
                .test()
                .expectNext(12.2f)
                .verifyComplete()
    }

    @Test
    fun floatArrayMax() {
        floatArrayOf(12.3f, 15f, 12.2f, 12.4f)
                .maxToMono()
                .test()
                .expectNext(15f)
                .verifyComplete()
    }



//== DoubleArray ==

    @Test
    fun doubleArraySum() {
        doubleArrayOf(3.5, 1.5)
                .sumToMono()
                .test()
                .expectNext(5.0)
                .verifyComplete()
    }

    @Test
    fun doubleArrayFloatSumCaps() {
        doubleArrayOf(Float.MAX_VALUE.toDouble(), 1.0)
                .floatSumToMono()
                .test()
                .expectNext(Float.MAX_VALUE)
                .verifyComplete()
    }

    @Test
    fun doubleArrayAverage() {
        doubleArrayOf(10.0, 11.0)
                .averageToMono()
                .test()
                .expectNext(10.5)
                .verifyComplete()
    }

    @Test
    fun doubleAverageFloat() {
        doubleArrayOf(10.0, 11.0)
                .floatAverageToMono()
                .test()
                .expectNext(10.5f)
                .verifyComplete()
    }

    @Test
    fun doubleArrayMin() {
        doubleArrayOf(12.3, 15.0, 12.2, 12.4)
                .minToMono()
                .test()
                .expectNext(12.2)
                .verifyComplete()
    }

    @Test
    fun doubleArrayMax() {
        doubleArrayOf(12.3, 15.0, 12.2, 12.4)
                .maxToMono()
                .test()
                .expectNext(15.0)
                .verifyComplete()
    }

// == collection of numbers ==

    @Test
    fun numberCollectionSum() {
        val longs: List<Long> = listOf(1L, 2L, 3L)
        val floats: List<Float> = listOf(1.5f, 2.5f)
        val doubles: List<Double> = listOf(1.6, 2.6)

        Flux.concat(
                longs.sumToMono(),
                floats.sumToMono(),
                doubles.sumToMono())
                .test()
                .expectNext(6L)
                .expectNext(3L).`as`("floats rounded down")
                .expectNext(3L).`as`("doubles rounded down")
                .verifyComplete()
    }

    @Test
    fun numberCollectionDoubleSum() {
        val longs: List<Long> = listOf(1L, 2L, 3L)
        //avoid weird 1.6f == 1.600000023841858 precision artifacts
        val floats: List<Float> = listOf(1.5f, 2.5f)
        val doubles: List<Double> = listOf(1.6, 2.6)

        Flux.concat(
                longs.doubleSumToMono(),
                floats.doubleSumToMono(),
                doubles.doubleSumToMono())
                .test()
                .expectNext(6.0)
                .expectNext(4.0).`as`("floats")
                .expectNext(4.2).`as`("doubles")
                .verifyComplete()
    }

    @Test
    fun numberCollectionAverage() {
        val longs: List<Long> = listOf(10L, 11L, 12L)
        val floats: List<Float> = listOf(10f, 11f, 12f)
        val doubles: List<Double> = listOf(10.0, 11.0, 12.0)

        Flux.concat(
                longs.averageToMono(),
                floats.averageToMono(),
                doubles.averageToMono())
                .test()
                .expectNext(11.0).`as`("longs")
                .expectNext(11.0).`as`("floats")
                .expectNext(11.0).`as`("doubles")
                .verifyComplete()
    }

// == collection and arrays of comparables ==

    @Test
    fun minComparableCollection() {
        comparableList.minToMono()
                .test()
                .expectNext("A")
                .verifyComplete()
    }

    @Test
    fun maxComparableCollection() {
        comparableList.maxToMono()
                .test()
                .expectNext("BB")
                .verifyComplete()
    }

    @Test
    fun minComparableArray() {
        comparableArray.minToMono()
                .test()
                .expectNext("A")
                .verifyComplete()
    }

    @Test
    fun maxComparableArray() {
        comparableArray.maxToMono()
                .test()
                .expectNext("BB")
                .verifyComplete()
    }

// == collection and arrays with comparators ==

    @Test
    fun minCollectionWithComparator() {
        userList.minToMono(comparator)
                .map { it.name }
                .test()
                .expectNext("baby")
                .verifyComplete()
    }

    @Test
    fun maxCollectionWithComparator() {
        userList.maxToMono(comparator)
                .map { it.name }
                .test()
                .expectNext("grandpa")
                .verifyComplete()
    }

    @Test
    fun minArrayWithComparator() {
        userArray.minToMono(comparator)
                .map { it.name }
                .test()
                .expectNext("baby")
                .verifyComplete()
    }

    @Test
    fun maxArrayWithComparator() {
        userArray.maxToMono(comparator)
                .map { it.name }
                .test()
                .expectNext("grandpa")
                .verifyComplete()
    }

// == collection with mapping to Number ==

    @Test
    fun sumMappedCollection() {
        userList.mapSumToMono { it -> it.age }
                .test()
                .expectNext(99)
                .verifyComplete()
    }

    @Test
    fun doubleSumMappedCollection() {
        userList.mapDoubleSumToMono { it -> it.age }
                .test()
                .expectNext(99.0)
                .verifyComplete()
    }

    @Test
    fun averageMappedCollection() {
        userList.mapAverageToMono { it -> it.age }
                .test()
                .expectNext(33.0)
                .verifyComplete()
    }

}