package reactor.math

import org.junit.Test
import reactor.core.publisher.Flux
import reactor.kotlin.core.publisher.toFlux
import reactor.kotlin.test.test

/**
 * @author Simon Baslé
 */
class MathFluxExtensionsTests {

    data class User(val age: Int,val name: String)

    companion object {
        val userList = listOf(User(18, "bob"),
                User(80, "grandpa"),
                User(1, "baby"))

        val comparator: Comparator<User> = Comparator({ u1: User, u2:User -> u1.age - u2.age})

        val comparableList = listOf("AA", "A", "BB", "B", "AB")
    }

    //== sum ==
    @Test
    fun sumShorts() {
        shortArrayOf(32_000, 8_000) //sum overflows a Short
                .toFlux()
                .sum()
                .test()
                .expectNext(40_000)
                .verifyComplete()
    }

    @Test
    fun sumInts() {
        intArrayOf(2_000_000_000, 200_000_000) //sum overflows an Int
                .toFlux()
                .sum()
                .test()
                .expectNext(2_200_000_000)
                .verifyComplete()
    }

    @Test
    fun sumLongs() {
        longArrayOf(3_000_000_000, 2_000_000_000)
                .toFlux()
                .sum()
                .test()
                .expectNext(5_000_000_000)
                .verifyComplete()
    }

    @Test
    fun sumFloatsPrecisionLoss() {
        floatArrayOf(3.5f, 1.5f)
                .toFlux()
                .sum()
                .test()
                .expectNext(4)
                .verifyComplete()
    }

    @Test
    fun sumDoublesPrecisionLoss() {
        doubleArrayOf(3.5, 1.5)
                .toFlux()
                .sum()
                .test()
                .expectNext(4)
                .verifyComplete()
    }

    @Test
    fun sumMapped() {
        userList.toFlux()
                .sum { it.age }
                .test()
                .expectNext(99)
                .verifyComplete()
    }

    //== sumDouble ==
    @Test
    fun sumDoubleShorts() {
        shortArrayOf(32_000, 8_000) //sum overflows a Short
                .toFlux()
                .sumDouble()
                .test()
                .expectNext(40_000.0)
                .verifyComplete()
    }

    @Test
    fun sumDoubleInts() {
        intArrayOf(2_000_000_000, 200_000_000) //sum overflows an Int
                .toFlux()
                .sumDouble()
                .test()
                .expectNext(2_200_000_000.0)
                .verifyComplete()
    }

    @Test
    fun sumDoubleLongs() {
        longArrayOf(3_000_000_000, 2_000_000_000)
                .toFlux()
                .sumDouble()
                .test()
                .expectNext(5_000_000_000.0)
                .verifyComplete()
    }

    @Test
    fun sumDoubleFloats() {
        floatArrayOf(3.5f, 1.5f)
                .toFlux()
                .sumDouble()
                .test()
                .expectNext(5.0)
                .verifyComplete()
    }

    @Test
    fun sumDoubleDoubles() {
        doubleArrayOf(3.5, 1.5)
                .toFlux()
                .sumDouble()
                .test()
                .expectNext(5.0)
                .verifyComplete()
    }

    @Test
    fun sumDoubleMapped() {
        userList.toFlux()
                .sumDouble { it.age }
                .test()
                .expectNext(99.0)
                .verifyComplete()
    }

    //== average ==
    @Test
    fun averageShorts() {
        shortArrayOf(10, 11)
                .toFlux()
                .average()
                .test()
                .expectNext(10.5)
                .verifyComplete()
    }

    @Test
    fun averageInts() {
        intArrayOf(10, 11)
                .toFlux()
                .average()
                .test()
                .expectNext(10.5)
                .verifyComplete()
    }

    @Test
    fun averageLongs() {
        longArrayOf(10, 11)
                .toFlux()
                .average()
                .test()
                .expectNext(10.5)
                .verifyComplete()
    }

    @Test
    fun averageFloats() {
        floatArrayOf(10f, 11f)
                .toFlux()
                .average()
                .test()
                .expectNext(10.5)
                .verifyComplete()
    }

    @Test
    fun averageDoubles() {
        doubleArrayOf(10.0, 11.0)
                .toFlux()
                .average()
                .test()
                .expectNext(10.5)
                .verifyComplete()
    }

    @Test
    fun averageMapped() {
        userList.toFlux()
                .average { it.age }
                .test()
                .expectNext(33.0)
                .verifyComplete()
    }

    //== min ==
    @Test
    fun minShorts() {
        shortArrayOf(12, 8, 16)
                .toFlux()
                .min()
                .test()
                .expectNext(8)
                .verifyComplete()
    }

    @Test
    fun minInts() {
        intArrayOf(12, 8, 16)
                .toFlux()
                .min()
                .test()
                .expectNext(8)
                .verifyComplete()
    }

    @Test
    fun minLongs() {
        longArrayOf(12, 8, 16)
                .toFlux()
                .min()
                .test()
                .expectNext(8)
                .verifyComplete()
    }

    @Test
    fun minFloats() {
        floatArrayOf(12f, 8.1f, 16f, 8.2f)
                .toFlux()
                .min()
                .test()
                .expectNext(8.1f)
                .verifyComplete()
    }

    @Test
    fun minDoubles() {
        doubleArrayOf(12.0, 8.1, 16.0, 8.2)
                .toFlux()
                .min()
                .test()
                .expectNext(8.1)
                .verifyComplete()
    }

    @Test
    fun minComparables() {
        comparableList.toFlux()
                .min()
                .test()
                .expectNext("A")
                .verifyComplete()
    }

    @Test
    fun minComparator() {
        userList.toFlux()
                .min(comparator)
                .map { it.name }
                .test()
                .expectNext("baby")
                .verifyComplete()
    }

    @Test
    fun minComparatorLambdaInverted() {
        userList.toFlux()
                .min { a, b -> b.age - a.age }
                .map { it.name }
                .test()
                .expectNext("grandpa")
                .verifyComplete()
    }

    //== max ==
    @Test
    fun maxShorts() {
        shortArrayOf(12, 8, 16)
                .toFlux()
                .max()
                .test()
                .expectNext(16)
                .verifyComplete()
    }

    @Test
    fun maxInts() {
        intArrayOf(12, 8, 16)
                .toFlux()
                .max()
                .test()
                .expectNext(16)
                .verifyComplete()
    }

    @Test
    fun maxLongs() {
        longArrayOf(12, 8, 16)
                .toFlux()
                .max()
                .test()
                .expectNext(16)
                .verifyComplete()
    }

    @Test
    fun maxFloats() {
        floatArrayOf(12f, 8.1f, 16f, 8.2f)
                .toFlux()
                .max()
                .test()
                .expectNext(16f)
                .verifyComplete()
    }

    @Test
    fun maxDoubles() {
        doubleArrayOf(12.0, 8.1, 16.0, 8.2)
                .toFlux()
                .max()
                .test()
                .expectNext(16.0)
                .verifyComplete()
    }

    @Test
    fun maxComparables() {
        comparableList.toFlux()
                .max()
                .test()
                .expectNext("BB")
                .verifyComplete()
    }

    @Test
    fun maxComparator() {
        userList.toFlux()
                .max(comparator)
                .map { it.name }
                .test()
                .expectNext("grandpa")
                .verifyComplete()
    }

    @Test
    fun maxComparatorLambdaInverted() {
        userList.toFlux()
                .max { a, b -> b.age - a.age }
                .map { it.name }
                .test()
                .expectNext("baby")
                .verifyComplete()
    }

//// == collection of numbers ==

    @Test
    fun numberCollectionSum() {
        val longs: List<Long> = listOf(1L, 2L, 3L)
        val floats: List<Float> = listOf(1.5f, 2.5f)
        val doubles: List<Double> = listOf(1.6, 2.6)

        Flux.concat(
                longs.toFlux().sum(),
                floats.toFlux().sum(),
                doubles.toFlux().sum())
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
                longs.toFlux().sumDouble(),
                floats.toFlux().sumDouble(),
                doubles.toFlux().sumDouble())
                .test()
                .expectNext(6.0)
                .expectNext(4.0).`as`("floats")
                .expectNext(4.2).`as`("doubles")
                .verifyComplete()
    }

}