package twitterstats

import java.time.ZonedDateTime

import cats.effect._
import cats.implicits._
import cats.{Semigroup, Show, derived}
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

    val printer: Stream[IO, Unit] = Stream.resource(Blocker[IO]).flatMap {
      blocker =>
        new Twitter[IO].tweetStream(request)
          .map(Twitter.parse)
          .collect { case Right(tweet) => tweet }
          .scan(WindowedAverage[Double](0, ZonedDateTime.now, 2.seconds))(
            (avg, tweet) => avg.addValue(1, tweet.timestamp))
          .through(stdoutLines(blocker))
          .take(100)
    }

    printer.compile.drain.as(ExitCode.Success)
  }
}
