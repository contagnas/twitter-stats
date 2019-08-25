package twitterstats

import java.time.ZonedDateTime

import cats.effect._
import cats.implicits._
import cats.{Monoid, Show, derived}
import fs2.Stream
import fs2.io.stdoutLines
import io.circe.Json
import io.circe.jawn.CirceSupportParser
import org.http4s._
import org.http4s.client.oauth1
import org.typelevel.jawn.RawFacade
import twitterstats.Twitter.Tweet

import scala.concurrent.duration._

private object Main extends IOApp {

  implicit val circeSupportParser: RawFacade[Json] = new CirceSupportParser(None, false).facade
  implicit def showWindowedAverage[A: Show]: Show[WindowedSum[A]] = (t: WindowedSum[A]) => t.toString + "\n"

  def run(args: List[String]): IO[ExitCode] = {
    val config = ArgParser.parseConfig(args)

    val request = oauth1.signRequest[IO](
      req = Request(uri = config.statusUri),
      consumer = config.consumer,
      callback = None,
      verifier = None,
      token = Some(config.token)
    )

    case class Stats(
      count: Int,
      hashtags: Map[String, Int]
    )

    def tweetToStats(tweet: Tweet): Stats = Stats(
      count = 1,
      hashtags = tweet.hashtags.map(_ -> 1).toMap
    )

    implicit val monoidStats: Monoid[Stats] = derived.semi.monoid
    implicit val showStats: Show[Stats] = (t: Stats) =>
      s"""
       |Count: ${t.count}
       |Hashtags: {
       |  ${t.hashtags.toList.sortBy { case (_, v) => -v }.take(5).map { case (k, v) => s"$k -> ${v.ceil.toInt}" }.mkString(",\n  ")}
       |}
       |""".stripMargin

    val printer: Stream[IO, Unit] = Stream.resource(Blocker[IO]).flatMap {
      blocker =>
        new Twitter[IO].tweetStream(request)
          .map(Twitter.parse)
          .collect { case Right(tweet) => tweet }
          .scan(WindowedSum.of(Monoid[Stats].empty, ZonedDateTime.now, 1.hour))(
            (avg, tweet) => avg.addValue(tweetToStats(tweet), tweet.timestamp))
          .map(_.valueForLast(2.seconds))
          .through(stdoutLines(blocker))
          .take(1000)
    }

    printer.compile.drain.as(ExitCode.Success)
  }
}
