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
  val lastTimestamp: ZonedDateTime = buckets.head.timestamp
  lazy val firstTimestamp: ZonedDateTime = buckets.find(_.value != Monoid.empty[A])
    .map(_.timestamp)
    .getOrElse(lastTimestamp)

  def value: A = Monoid[A].combineAll(buckets.map(_.value))

  def shrinkWindowTo(duration: Duration, endTime: ZonedDateTime = lastTimestamp): WindowedSum[A] =
    new WindowedSum(
      getBucketsForWindowEndingAt(WindowedSum.roundTimestamp(endTime), duration),
      maxWindow
    )

  def addValue(valueToAdd: A, timestamp: ZonedDateTime): WindowedSum[A] = {
    val roundedTimestamp = WindowedSum.roundTimestamp(timestamp)

    // slide the buckets to this value
    val adjustedBucketWindow = getBucketsForWindowEndingAt(roundedTimestamp, maxWindow)

    // The values might not come in order, so put this in the correct bucket.
    // Since each bucket is one second, the seconds before the latest timestamp is the index
    val index = ChronoUnit.SECONDS.between(roundedTimestamp, adjustedBucketWindow.head.timestamp).toInt

    val newBuckets = adjustedBucketWindow.get(index)
      .map(bucket => adjustedBucketWindow.updated(index, bucket.copy(value = bucket.value |+| valueToAdd)))
      .getOrElse(adjustedBucketWindow)

    new WindowedSum(newBuckets, maxWindow)
  }

  private def getBucketsForWindowEndingAt(endTime: ZonedDateTime, windowSize: Duration): Vector[Bucket[A]] = {
    val roundedEndTime = WindowedSum.roundTimestamp(endTime)

    val secondDiff = ChronoUnit.SECONDS.between(lastTimestamp, endTime).toInt

    // Only slide forward in time, don't slide more than is needed to replace all buckets
    val slideAmount = math.min(buckets.length, math.max(0, secondDiff))

    val newBuckets = (0 until slideAmount)
      .map(s => Bucket(timestamp = endTime.minusSeconds(s), value = Monoid[A].empty))
      .toVector

    (newBuckets ++ buckets.dropRight(slideAmount)).take(windowSize.toSeconds.toInt)
  }
}

object WindowedSum {
  private val RESOLUTION = 1.second

  def of[A: Monoid](startingValue: A, timestamp: ZonedDateTime, maxWindow: Duration): WindowedSum[A] = {
    assert(maxWindow > RESOLUTION, s"max window must be greater than $RESOLUTION")
    val roundedTimestamp = roundTimestamp(timestamp)
    val buckets: Vector[Bucket[A]] = (1 until (maxWindow / RESOLUTION).toInt).map {
      s => Bucket(roundedTimestamp.minusSeconds(s), Monoid.empty[A])
    }.toVector

    val initialBucket = Bucket(roundedTimestamp, startingValue)
    new WindowedSum(buckets.+:(initialBucket), maxWindow)
  }

  private def roundTimestamp(timestamp: ZonedDateTime): ZonedDateTime =
    timestamp.truncatedTo(ChronoUnit.SECONDS)

}

private case class Bucket[A](timestamp: ZonedDateTime, value: A)
