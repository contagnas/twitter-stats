package twitterstats

import java.time.ZonedDateTime

import cats.effect._
import cats.implicits._
import cats.{Monoid, Show, derived}
import fs2.Stream
import fs2.io.stdoutLines
import io.circe.Json
import io.circe.jawn.CirceSupportParser
import org.http4s.Uri.Host
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
      hashtags: Map[String, Int],
      tweetsContainingUrls: Int,
      domains: Map[Host, Int],
      mediaTypes: Map[String, Int],
      mediaDomains: Map[Host, Int],
    )

    def count[A](as: List[A]): Map[A, Int] = as.groupBy(identity)
      .map { case (k, vs) => k -> vs.length}

    def tweetToStats(tweet: Tweet): Stats = Stats(
      count = 1,
      hashtags = count(tweet.hashtags),
      tweetsContainingUrls = if (tweet.urls.nonEmpty) 1 else 0,
      domains = count(tweet.urls.flatMap(_.host)),
      mediaTypes = count(tweet.mediaUrls.map(_.mediaType)),
      mediaDomains = count(tweet.mediaUrls.flatMap(_.url.host))
    )

    implicit val monoidStats: Monoid[Stats] = derived.semi.monoid
    implicit val showStats: Show[Stats] = (t: Stats) =>
      s"""
       |Count: ${t.count}
       |Hashtags: {
       |  ${t.hashtags.toList.sortBy { case (_, v) => -v }.take(5).map { case (k, v) => s"$k -> ${v.ceil.toInt}" }.mkString(",\n  ")}
       |}
       |Contain Urls: ${t.tweetsContainingUrls}
       |Domains: ${t.domains}
       |Media Domains: ${t.mediaDomains}
       |Media Types: ${t.mediaTypes}
       |""".stripMargin

    val printer: Stream[IO, Unit] = Stream.resource(Blocker[IO]).flatMap {
      blocker =>
        new Twitter[IO].tweetStream(request)
          .map(Twitter.parse)
          .collect { case Right(tweet) => tweet }
          .scan(WindowedSum.of(Monoid[Stats].empty, ZonedDateTime.now, 1.hour))(
            (avg, tweet) => avg.addValue(tweetToStats(tweet), tweet.timestamp))
          .map(_.valueForLast(30.seconds))
          .through(stdoutLines(blocker))
          .take(1000)
    }

    printer.compile.drain.as(ExitCode.Success)
  }
}
