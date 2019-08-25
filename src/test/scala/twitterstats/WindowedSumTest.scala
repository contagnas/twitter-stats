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
    avg.addValue(50, endTime).valueForLast(1.second) shouldBe 50
  }

//  it should "handle out-of-order timestamps outside of window" in {
//    val endTime = startTime.plusMinutes(60)
//    val avg = WindowedAverage.of(100, endTime, 1.hour)
//    avg.addValue(50, startTime).valueForLast(1.minute) shouldBe 100
//  }

  it should "handle out-of-order timestamps inside of window" in {
    val endTime = startTime.plusSeconds(15)
    val avg = WindowedSum.of(100, endTime, 1.hour)
    avg.addValue(50, startTime).valueForLast(1.hour) shouldBe 150
  }

  it should "return reasonable results" in {
    val iterations = 100
    val duration = 30

    val windowedAverage = (1 to iterations).foldLeft(WindowedSum.of(0, startTime, duration.minutes)) { (avg, i) =>
      val timestamp = startTime.plusMinutes(i)
      avg.addValue(1, timestamp)
    }

    windowedAverage.lastTimestamp shouldEqual startTime.plusMinutes(iterations)
    windowedAverage.valueForLast(1.hour) shouldEqual duration
  }

  it should "produce the same result with multiple small values or one large value" in {
    val startingAvg = WindowedSum.of(0, startTime, 1.hour)
    val numberOfEvents = 100
    val repeatedAdd = (1 to numberOfEvents).foldLeft(startingAvg) (
      (avg, _) => avg.addValue(1, startTime)
    )
    val addOnce = startingAvg.addValue(numberOfEvents, startTime)
    repeatedAdd.valueForLast(1.second) shouldEqual addOnce.valueForLast(1.second)
  }
}
