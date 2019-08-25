package twitterstats

import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import cats.implicits._
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class WindowedAverageTest extends FlatSpec with Matchers {
  val startTime: ZonedDateTime = ZonedDateTime.of(
    LocalDateTime.of(2019, 1, 1, 1, 0),
    ZoneOffset.UTC
  )

  "WindowedAverage" should "handle time gaps greater than the window" in {
    val endTime = startTime.plusMinutes(60)
    val avg = WindowedAverage[Int](100, startTime, 30.seconds)
    avg.addValue(50, endTime).value shouldBe 50
  }

  it should "handle out-of-order timestamps outside of window" in {
    val endTime = startTime.plusMinutes(60)
    val avg = WindowedAverage[Int](100, endTime, 30.seconds)
    avg.addValue(50, startTime).value shouldBe 100
  }

  it should "handle out-of-order timestamps inside of window" in {
    val endTime = startTime.plusSeconds(15)
    val avg = WindowedAverage[Int](100, endTime, 30.seconds)
    avg.addValue(50, startTime).value shouldBe 150
  }

  it should "return reasonable results" in {
    val iterations = 100
    val duration = 30

    val windowedAverage = (1 to iterations).foldLeft(WindowedAverage(0, startTime, duration.minutes)) { (avg, i) =>
      val timestamp = startTime.plusMinutes(i)
      avg.addValue(1, timestamp)
    }

    windowedAverage.lastTimeStamp shouldEqual startTime.plusMinutes(iterations)
    windowedAverage.value shouldEqual duration
  }

  it should "produce the same result with multiple small values or one large value" in {
    val startingAvg = WindowedAverage[Int](0, startTime, 1.minute)
    val numberOfEvents = 100
    val repeatedAdd = (1 to numberOfEvents).foldLeft(startingAvg) (
      (avg, i) => avg.addValue(1, startTime)
    )
    val addOnce = startingAvg.addValue(numberOfEvents, startTime)
    repeatedAdd shouldEqual addOnce
  }
}
