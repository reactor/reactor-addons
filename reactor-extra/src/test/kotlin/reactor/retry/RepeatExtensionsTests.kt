package reactor.retry

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.Duration

class RepeatExtensionsTests {

    @Test
    fun fluxRepeatExponentialBackoff() {
        val repeats = mutableListOf<Long>()

        val flux = Flux.range(0, 2)
                .repeatExponentialBackoff(4, Duration.ofMillis(100),
                        Duration.ofMillis(500)) { repeats.add(it.backoff().toMillis()) }

        StepVerifier.withVirtualTime { flux }
                .expectNext(0, 1)
                .expectNoEvent(Duration.ofMillis(50))  // delay=100
                .thenAwait(Duration.ofMillis(100))
                .expectNext(0, 1)
                .expectNoEvent(Duration.ofMillis(150)) // delay=200
                .thenAwait(Duration.ofMillis(100))
                .expectNext(0, 1)
                .expectNoEvent(Duration.ofMillis(250)) // delay=400
                .thenAwait(Duration.ofMillis(100))
                .expectNext(0, 1)
                .expectNoEvent(Duration.ofMillis(450)) // delay=500
                .thenAwait(Duration.ofMillis(100))
                .expectNext(0, 1)
                .verifyComplete()

        assertThat(repeats).containsExactly(100L, 200L, 400L, 500L)
    }

    @Test
    fun monoRepeatExponentialBackoff() {
        val repeats = mutableListOf<Long>()

        val flux = Mono.just(0)
                .repeatExponentialBackoff(4, Duration.ofMillis(100),
                        Duration.ofMillis(500)) { repeats.add(it.backoff().toMillis()) }

        StepVerifier.withVirtualTime { flux }
                .expectNext(0)
                .thenAwait(Duration.ofMillis(100))
                .expectNext(0)
                .thenAwait(Duration.ofMillis(200))
                .expectNext(0)
                .thenAwait(Duration.ofMillis(400))
                .expectNext(0)
                .thenAwait(Duration.ofMillis(500))
                .expectNext(0)
                .verifyComplete()

        assertThat(repeats).containsExactly(100L, 200L, 400L, 500L)
    }

    @Test
    fun fluxRepeatRandomBackoff() {
        val repeats = mutableListOf<Long>()

        val flux = Flux.range(0, 2)
                .repeatRandomBackoff(4, Duration.ofMillis(100),
                        Duration.ofMillis(500)) { repeats.add(it.backoff().toMillis())}

        StepVerifier.withVirtualTime { flux }
                .expectNext(0, 1)
                .expectNoEvent(Duration.ofMillis(90))
                .thenAwait(Duration.ofMillis(50))
                .expectNext(0, 1)
                .thenAwait(Duration.ofMillis(500))
                .expectNext(0, 1)
                .thenAwait(Duration.ofMillis(500))
                .expectNext(0, 1)
                .thenAwait(Duration.ofMillis(500))
                .expectNext(0, 1)
                .verifyComplete()

        val repeatFirstPass = repeats.toList()
        repeats.clear()
        StepVerifier.withVirtualTime { flux }
                .thenAwait(Duration.ofHours(1))
                .expectNext(0, 1, 0, 1, 0, 1, 0, 1, 0, 1)
                .verifyComplete()

        assertThat(repeatFirstPass)
                .hasSize(4)
                .startsWith(100)

        assertThat(repeats)
                .startsWith(100)
                .hasSize(4)

        assertThat(repeats.minus(repeatFirstPass))
                .describedAs("second pass has at least one different random delay")
                .isNotEmpty
    }


    @Test
    fun monoRepeatRandomBackoff() {
        val repeats = mutableListOf<Long>()

        val flux = Mono.just(0)
                .repeatRandomBackoff(4, Duration.ofMillis(100),
                        Duration.ofMillis(2000)) { repeats.add(it.backoff().toMillis()) }

        StepVerifier.withVirtualTime { flux }
                .expectNext(0)
                .thenAwait(Duration.ofMillis(100))
                .expectNext(0)
                .thenAwait(Duration.ofMillis(2000))
                .expectNext(0)
                .thenAwait(Duration.ofMillis(2000))
                .expectNext(0)
                .thenAwait(Duration.ofMillis(2000))
                .expectNext(0)
                .verifyComplete()

        val repeatFirstPass = repeats.toList()
        repeats.clear()
        StepVerifier.withVirtualTime { flux }
                .thenAwait(Duration.ofHours(1))
                .expectNext(0,0,0,0,0)
                .verifyComplete()

        assertThat(repeatFirstPass)
                .hasSize(4)
                .startsWith(100)

        assertThat(repeats)
                .startsWith(100)
                .hasSize(4)
                .hasSize(4)

        assertThat(repeats.minus(repeatFirstPass))
                .describedAs("second pass has at least one different random delay")
                .isNotEmpty
    }
}