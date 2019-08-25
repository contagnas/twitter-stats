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
  implicit def showWindowedAverage[A: Show]: Show[WindowedAverage[A]] = (t: WindowedAverage[A]) => t.toString + "\n"

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
      count: Double,
      hashtags: Map[String, Double]
    )

    def tweetToStats(tweet: Tweet): Stats = Stats(
      count = 1d,
      hashtags = tweet.hashtags.map(_ -> 1d).toMap
    )

    implicit val monoidStats: Monoid[Stats] = derived.semi.monoid
    implicit val showStats: Show[Stats] = (t: Stats) =>
      s"""
       |Count: ${t.count}
       |Hashtags: {
       |  ${t.hashtags.toList.sortBy { case (_, v) => -v }.map { case (k, v) => s"$k -> ${v.ceil.toInt}" }.mkString(",\n  ")}
       |}
       |""".stripMargin

    implicit val decayableStats: Decayable[Stats] = DeriveDecayable.decayable
    def showWA[A: Show]: Show[WindowedAverage[A]] =
      (t: WindowedAverage[A]) => Show[A].show(t.value)

    val printer: Stream[IO, Unit] = Stream.resource(Blocker[IO]).flatMap {
      blocker =>
        new Twitter[IO].tweetStream(request)
          .map(Twitter.parse)
          .collect { case Right(tweet) => tweet }
          .scan(WindowedAverage[Stats](Monoid[Stats].empty, ZonedDateTime.now, 1.minute))(
            (avg, tweet) => avg.addValue(tweetToStats(tweet), tweet.timestamp))
          .map(showWA[Stats].show)
          .through(stdoutLines(blocker))
          .take(1000)
    }

    printer.compile.drain.as(ExitCode.Success)
  }
}
