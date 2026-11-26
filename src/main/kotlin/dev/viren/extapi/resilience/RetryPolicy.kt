package dev.viren.extapi.resilience

import dev.viren.extapi.config.RetryProperties
import java.time.Duration
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import kotlin.math.min
import kotlin.math.pow

/**
 * Exponential backoff with jitter. With `jitter = 0.5` the sleep for attempt n is a random value
 * in `[0.5 * base, base]` where `base = initial * multiplier^(n-1)`, capped at `maxBackoff`.
 * A Retry-After from the upstream wins over the computed value (it knows its own recovery).
 */
class RetryPolicy(private val props: RetryProperties) {

    /** @param attempt the attempt that just failed, starting at 1. */
    fun backoff(attempt: Int, retryAfter: Duration?): Duration {
        if (retryAfter != null && props.respectRetryAfter) return retryAfter
        val base = props.initialBackoff.toMillis() * props.multiplier.pow(attempt - 1)
        val capped = min(base, props.maxBackoff.toMillis().toDouble())
        val jitter = props.jitter.coerceIn(0.0, 1.0)
        val low = capped * (1 - jitter)
        val millis = if (jitter == 0.0) capped else ThreadLocalRandom.current().nextDouble(low, capped + 1)
        return Duration.ofMillis(millis.toLong())
    }

    fun retryAfterTooLong(retryAfter: Duration?): Boolean =
        retryAfter != null && props.respectRetryAfter && retryAfter > props.maxRetryAfter

    companion object {
        /** Exponential schedule for the async outbox (no jitter needed: rows are spread by time already). */
        fun exponential(attempt: Int, initial: Duration, max: Duration): Duration {
            val millis = initial.toMillis() * 2.0.pow((attempt - 1).coerceAtMost(30))
            val withJitter = millis * ThreadLocalRandom.current().nextDouble(0.8, 1.2)
            return Duration.ofMillis(min(withJitter, max.toMillis().toDouble()).toLong())
        }
    }
}

/** Caps concurrent in-flight calls so one slow upstream cannot exhaust every request thread. */
class Bulkhead(val maxConcurrent: Int, private val maxWait: Duration) {
    private val semaphore = Semaphore(maxConcurrent, true)

    fun tryEnter(): Boolean =
        if (maxWait.isZero) semaphore.tryAcquire() else semaphore.tryAcquire(maxWait.toNanos(), TimeUnit.NANOSECONDS)

    fun exit() = semaphore.release()

    val available: Int get() = semaphore.availablePermits()
}
