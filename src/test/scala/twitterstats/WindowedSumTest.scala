package twitterstats

import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import cats.implicits._
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class WindowedSumTest extends FlatSpec with Matchers {
  val startTime: ZonedDateTime = ZonedDateTime.of(
    LocalDateTime.of(2019, 1, 1, 1, 0),
    ZoneOffset.UTC
  )

  "WindowedAverage" should "handle time gaps greater than the window" in {
    val endTime = startTime.plusMinutes(60)
    val avg = WindowedSum.of(100, startTime, 1.minute)
    avg.addValue(50, endTime).shrinkWindowTo(1.second).value shouldBe 50
  }

  it should "handle out-of-order timestamps outside of window" in {
    val endTime = startTime.plusMinutes(60)
    val avg = WindowedSum.of(100, endTime, 1.hour)
    avg.addValue(50, startTime).shrinkWindowTo(1.minute).value shouldBe 100
  }

  it should "handle out-of-order timestamps inside of window" in {
    val endTime = startTime.plusSeconds(15)
    val avg = WindowedSum.of(100, endTime, 1.hour)
    avg.addValue(50, startTime).shrinkWindowTo(1.hour).value shouldBe 150
  }

  it should "return reasonable results" in {
    val iterations = 100
    val duration = 30

    val windowedAverage = (1 to iterations).foldLeft(WindowedSum.of(0, startTime, duration.minutes)) { (sum, i) =>
      val timestamp = startTime.plusMinutes(i)
      sum.addValue(1, timestamp)
    }

    windowedAverage.lastTimestamp shouldEqual startTime.plusMinutes(iterations)
    windowedAverage.shrinkWindowTo(1.hour).value shouldEqual duration
  }

  it should "handle windows correctly" in {
    val empty = WindowedSum.of(0, startTime, 1.hour)
    val windowSum = (1 to 100).foldLeft(empty) { (sum, i) =>
      sum.addValue(i, startTime.plusSeconds(i))
    }

    windowSum.shrinkWindowTo(10.seconds).value should be ((91 to 100).sum)
  }

  it should "produce the same result with multiple small values or one large value" in {
    val startingAvg = WindowedSum.of(0, startTime, 1.hour)
    val numberOfEvents = 100
    val repeatedAdd = (1 to numberOfEvents).foldLeft(startingAvg) (
      (avg, _) => avg.addValue(1, startTime)
    )
    val addOnce = startingAvg.addValue(numberOfEvents, startTime)
    repeatedAdd.shrinkWindowTo(1.second).value shouldEqual addOnce.shrinkWindowTo(1.second).value
  }
}
