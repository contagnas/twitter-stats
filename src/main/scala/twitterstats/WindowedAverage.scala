package twitterstats

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import cats.Semigroup
import cats.implicits._

import scala.concurrent.duration._

/*
 * Given a stream timestamped data, approximate how many of them occur in the
 * last second, minute, hour, etc.
 *
 * This implementation assumes that existing average is from a uniform
 * distribution which probably isn't great for a stream of tweets, but it is very
 * simple and should be fine over relatively small time horizons.
 */
case class WindowedAverage[A: Decayable: Semigroup](
  value: A,
  lastTimeStamp: LocalDateTime,
  window: Duration
) {
  def addValue(
    addedValue: A,
    timestamp: LocalDateTime,
  ): WindowedAverage[A] = {
    val newValue = if (timestamp.isAfter(lastTimeStamp)) {
      val timeElapsed: Long = lastTimeStamp.until(timestamp, ChronoUnit.MILLIS)
      val decay: Double = math.min(math.max(timeElapsed.toDouble, 0.0) / this.window.toMillis, 1)
      Decayable[A].decay(value, decay) |+| addedValue
    } else {
      // Receiving timestamps out of order is weird, but put the value into the bucket if it falls in the bucket
      if (!timestamp.isBefore(lastTimeStamp.minusSeconds(this.window.toSeconds))) {
        value |+| addedValue
      } else {
        value
      }
    }

    this.copy(
      value = newValue,
      lastTimeStamp = timestamp
    )
  }
}

trait Decayable[A] {
  def decay(value: A, decayAmount: Double): A
}

object Decayable {
  def apply[A: Decayable]: Decayable[A] = implicitly[Decayable[A]]
  implicit val decayableDouble: Decayable[Double] =
    (value: Double, decayAmount: Double) => value * (1 - decayAmount)

  implicit val decayableInt: Decayable[Int] =
    (value: Int, decayAmount: Double) =>
      Decayable[Double].decay(value.toDouble, decayAmount).ceil.toInt

}

object Decay {
  def addValue(
    nextValue: Int,
    timestamp: LocalDateTime,
    runningAverage: WindowedAverage[Int]
  ): WindowedAverage[Int] = {
    val timeElapsed: Long = runningAverage.lastTimeStamp.until(timestamp, ChronoUnit.SECONDS)
    val decay: Double = math.max(timeElapsed.toDouble, 0.0) / runningAverage.window.toSeconds
    val newValue = runningAverage.value * decay + nextValue
    runningAverage.copy(
      value = newValue.toInt,
      lastTimeStamp = timestamp
    )
  }
}
