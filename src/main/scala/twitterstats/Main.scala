package twitterstats

import java.time.ZonedDateTime

import cats.Monoid
import cats.effect._
import cats.implicits._
import fs2.Stream
import fs2.io.stdoutLines
import io.circe.Json
import io.circe.jawn.CirceSupportParser
import org.http4s._
import org.http4s.client.oauth1
import org.typelevel.jawn.RawFacade

import scala.concurrent.duration._

private object Main extends IOApp {

  implicit val circeSupportParser: RawFacade[Json] = new CirceSupportParser(None, false).facade

  def run(args: List[String]): IO[ExitCode] = {
    val config = ArgParser.parseConfig(args)

    val request = oauth1.signRequest[IO](
      req = Request(uri = config.statusUri),
      consumer = config.consumer,
      callback = None,
      verifier = None,
      token = Some(config.token)
    )

    val windowedSum = WindowedSum.of(Monoid[TweetStats].empty, ZonedDateTime.now, 1.hour)

    val printer: Stream[IO, Unit] = Stream.resource(Blocker[IO]).flatMap {
      blocker =>
        new Twitter[IO].tweetStream(request)
          .map(Twitter.parse)
          .collect { case Right(tweet) => tweet }
          .scan(windowedSum)((sum, tweet) => sum.addValue(TweetStats.of(tweet), tweet.timestamp))
          .map(_.valueForLast(30.seconds))
          .through(stdoutLines(blocker))
          .take(1000)
    }

    printer.compile.drain.as(ExitCode.Success)
  }
}
