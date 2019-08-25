package twitterstats

import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

import cats.Monoid
import cats.implicits._

import scala.concurrent.duration._

/**
 *  Track stats over a sliding window. Timestamps are dropped into buckets with a 1-second resolution
 * @param buckets buckets for each second recorded
 * @param maxWindow The duration buckets are allowed to stay in the window.
 * @tparam A the type of stats to track
 */
class WindowedSum[A: Monoid] private (buckets: Vector[Bucket[A]], maxWindow: Duration) {
  private def getBucketsInWindowEndingAt(endTime: ZonedDateTime, windowSize: Duration): Vector[Bucket[A]] = {
    val lastIndexInWindow = buckets.lastIndexWhere { bucket =>
      ChronoUnit.SECONDS.between(bucket.timestamp, endTime) < windowSize.toSeconds
    }
    buckets.take(lastIndexInWindow + 1)
  }

  val lastTimestamp: ZonedDateTime = buckets.head.timestamp

  def valueForLast(duration: Duration, now: ZonedDateTime = lastTimestamp): A =
    Monoid[A].combineAll(getBucketsInWindowEndingAt(now, duration).map(_.value))

  def addValue(
    addedValue: A,
    timestamp: ZonedDateTime,
  ): WindowedSum[A] = {
    val roundedTimestamp = WindowedSum.roundTimestamp(timestamp)
    val truncatedBuckets: Vector[Bucket[A]] = getBucketsInWindowEndingAt(roundedTimestamp, maxWindow)
    val headBucket = buckets.head

    val newBuckets = if (headBucket.timestamp.toInstant.equals(roundedTimestamp.toInstant)) {
      val newHead = headBucket.copy(value = headBucket.value |+| addedValue)
      val tailBuckets: Vector[Bucket[A]] = truncatedBuckets.tail
      tailBuckets.+:(newHead)
    } else {
      truncatedBuckets.+:(Bucket(roundedTimestamp, addedValue))
    }

    new WindowedSum(newBuckets, maxWindow)
  }
}

object WindowedSum {
  private val RESOLUTION = 1.second

  def of[A: Monoid](startingValue: A, timestamp: ZonedDateTime, maxWindow: Duration): WindowedSum[A] = {
    assert(maxWindow > RESOLUTION, s"max window must be greater than ${RESOLUTION}")
    new WindowedSum(Vector(Bucket(roundTimestamp(timestamp), startingValue)), maxWindow)
  }

  private def roundTimestamp(timestamp: ZonedDateTime): ZonedDateTime =
    timestamp.truncatedTo(ChronoUnit.SECONDS)
}

private case class Bucket[A](timestamp: ZonedDateTime, value: A)
