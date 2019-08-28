package twitterstats

import java.time.ZonedDateTime

import cats.Monoid
import cats.effect._
import cats.implicits._
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.http4s._
import org.http4s.client.oauth1
import org.http4s.implicits._
import org.http4s.server.Router
import org.http4s.server.blaze.BlazeServerBuilder

import scala.concurrent.duration._

private object Main extends IOApp {
  val maxWindow: FiniteDuration = 1.hour
  val windowedSum: WindowedSum[TweetStats] =
    WindowedSum.of(Monoid[TweetStats].empty, ZonedDateTime.now, maxWindow)

  def streamTweetsToSignal(
    statsSignal: SignallingRef[IO, WindowedSum[TweetStats]],
    request: IO[Request[IO]]
  ): Stream[IO, WindowedSum[TweetStats]] =
    new Twitter[IO].tweetStream(request)
      .map(Twitter.parse)
      .collect { case Right(tweet) => tweet }
      .scan(windowedSum)((sum, tweet) => sum.addValue(TweetStats.of(tweet), tweet.timestamp))
      .evalTap(statsSignal.set)

  def runServer(twitterStatsRoutes: HttpRoutes[IO]): Stream[IO, ExitCode] =
    BlazeServerBuilder[IO]
      .bindHttp(8080, "0.0.0.0")
      .withHttpApp(Router("/" -> twitterStatsRoutes).orNotFound)
      .serve

  def run(args: List[String]): IO[ExitCode] = {
    val config = ArgParser.parseConfig(args)

    val request = oauth1.signRequest[IO](
      req = Request(uri = config.statusUri),
      consumer = config.consumer,
      callback = None,
      verifier = None,
      token = Some(config.token)
    )

    val program = for {
      signal <- Stream.eval(SignallingRef[IO, WindowedSum[TweetStats]](windowedSum))
      tweetStream: Stream[IO, WindowedSum[TweetStats]] = streamTweetsToSignal(signal, request).drain
      server = runServer(new TwitterStatsApi(signal, Clock[IO]).routes)
      stream <- tweetStream.merge(server)
    } yield stream

    program.compile.drain.as(ExitCode.Success)
  }
}

