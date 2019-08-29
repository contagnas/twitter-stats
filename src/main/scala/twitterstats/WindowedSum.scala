package twitterstats

import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

import cats.Monoid
import cats.implicits._

import scala.concurrent.duration._

/**
  * Read-only version of WindowedSum. This is returned to prevent additional writes when the internal
  * number of buckets in a WindowedSum has been modified
  * @tparam A
  */
trait WindowedSumSlice[A] {
  val value: A
  val lastTimestamp: ZonedDateTime
  val firstTimestamp: ZonedDateTime
}

/**
 *  Tracks a sum over a sliding window.
  *
  *  Timestamps are dropped into buckets with a 1-second resolution.
  *
  *  Buckets are stored in reverse order, meaning buckets.head is the latest value.
  *
  *  Every second in the window is always tracked, meaning this class will always have
  *  buckets.length == maxWindow / 1 second
  *
  *
 * @param buckets buckets for each second recorded
 * @param maxWindow The duration buckets are allowed to stay in the window.
 * @tparam A the type of stats to track
 */
class WindowedSum[A: Monoid] private (buckets: Vector[Bucket[A]], maxWindow: Duration) extends WindowedSumSlice[A] {
  /**
   * The latest bucket in the window
   */
  override val lastTimestamp: ZonedDateTime = buckets.head.timestamp

  /**
    * The earliest non-empty bucket in the window
    */
  override lazy val firstTimestamp: ZonedDateTime = buckets
    .reverse
    .find(_.value != Monoid.empty[A])
    .map(_.timestamp)
    .getOrElse(lastTimestamp)

  /**
    * The sum value of this window
    */
  override lazy val value: A = Monoid[A].combineAll(buckets.map(_.value))

  /**
    * Shrink to a window of a given duration, ending at a given datetime
    * @param duration the duration of the window
    * @param endTime the last time in the window, defaulting to the last timestamp in the given window
    * @return The shortened window. Note that values cannot be added and this value cannot be shrunk further.
    */
  def shrinkWindowTo(duration: Duration, endTime: ZonedDateTime = lastTimestamp): WindowedSumSlice[A] =
    new WindowedSum(
      getBucketsForWindowEndingAt(WindowedSum.roundTimestamp(endTime), duration),
      maxWindow
    )

  /**
    * Add a value to the window.
    * @param valueToAdd the value to add
    * @param timestamp the time the value occured. If this time is after the current last time of
    *                  the window, its time will be the new last time of the window, possibly
    *                  dropping items earlier than the window's max window. If this time is before
    *                  current last window, it will be added to the existing value with its timestamp.
    *                  If it is earlier than the max window ago, it is dropped.
    * @return The window with the added value
    */
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
