package twitterstats

import cats.effect._
import cats.implicits._
import fs2.io.stdout
import fs2.{Stream, text}
import io.circe.Json
import io.circe.jawn.CirceSupportParser
import jawnfs2._
import org.http4s._
import org.http4s.client.blaze._
import org.http4s.client.oauth1
import org.http4s.client.oauth1.{Consumer, Token}
import org.http4s.implicits._
import org.typelevel.jawn.RawFacade

import scala.concurrent.ExecutionContext.global

private object Main extends IOApp {
  val apiKey = "secret"
  val apiSecretKey = "secret"
  val accessToken = "secret"
  val accessTokenSecret = "secret"

  val consumer = oauth1.Consumer(apiKey, apiSecretKey)
  val token = oauth1.Token(accessToken, accessTokenSecret)

  implicit val circeSupportParser: RawFacade[Json] = new CirceSupportParser(None, false).facade

  def signRequest(consumer: Consumer, token: Token)(req: Request[IO]): IO[Request[IO]] = {
    oauth1.signRequest(req, consumer, callback = None, verifier = None, token = Some(token))
  }

  val request: Request[IO] = Request(uri = uri"""https://stream.twitter.com/1.1/statuses/sample.json""")
  val oauthRequest: IO[Request[IO]] = oauth1.signRequest(
    req = request,
    consumer = consumer,
    callback = None,
    verifier = None,
    token = Some(token)
  )

  def run(args: List[String]): IO[ExitCode] = {
    val tweets: Stream[IO, Json] = for {
      client <- BlazeClientBuilder[IO](global).stream
      request <- Stream.eval(oauthRequest)
      tweets <- client.stream(request).flatMap(_.body.chunks.parseJsonStream)
    } yield tweets

    val printer: Stream[IO, Unit] = Stream.resource(Blocker[IO]).flatMap {
      blocker =>
        tweets
          .map(_.spaces2)
          .through(text.utf8Encode)
          .through(stdout(blocker))
    }

    printer.compile.drain.as(ExitCode.Success)
  }
}

