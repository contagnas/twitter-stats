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

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.global

private object Main extends IOApp {
  case class Config(
    token: Token,
    consumer: Consumer,
    statusUri: Uri
  )

  def parseConfig(args: List[String]): Config = {
    case class OConfig(
      apiKey: Option[String] = None,
      apiSecretKey: Option[String] = None,
      accessToken: Option[String] = None,
      accessTokenSecret: Option[String] = None,
      statusUri: Uri = uri"""https://stream.twitter.com/1.1/statuses/sample.json"""
    ) {
      def toConfig = Config(
        consumer = Consumer(apiKey.get, apiSecretKey.get),
        token = Token(accessToken.get, accessTokenSecret.get),
        statusUri = statusUri
      )
    }

    @tailrec
    def parse(args: List[String], config: OConfig): OConfig =
      args match {
        case Nil => config
        case "--apiKey" :: apiKey :: rest => parse(rest, config.copy(apiKey = Some(apiKey)))
        case "--apiSecretKey" :: apiSecretKey :: rest => parse(rest, config.copy(apiSecretKey = Some(apiSecretKey)))
        case "--accessToken" :: accessToken :: rest => parse(rest, config.copy(accessToken = Some(accessToken)))
        case "--accessTokenSecret" :: accessTokenSecret :: rest => parse(rest, config.copy(accessTokenSecret = Some(accessTokenSecret)))
        case "--statusUri" :: statusUri :: rest => parse(rest, config.copy(statusUri = Uri.unsafeFromString(statusUri)))
        case _ => throw new IllegalArgumentException("Bad arguments")
      }

    parse(args, OConfig()).toConfig
  }

  implicit val circeSupportParser: RawFacade[Json] = new CirceSupportParser(None, false).facade

  def run(args: List[String]): IO[ExitCode] = {
    val config = parseConfig(args)

    val request = oauth1.signRequest[IO](
      req = Request(uri = config.statusUri),
      consumer = config.consumer,
      callback = None,
      verifier = None,
      token = Some(config.token)
    )

    val tweets: Stream[IO, String] = for {
      client <- BlazeClientBuilder[IO](global).stream
      request <- Stream.eval(request)
      rawTweet <- client.stream(request)
      tweet <- rawTweet.body.chunks.parseJsonStream
      prettyTweet = tweet.spaces4
    } yield prettyTweet

    val printer: Stream[IO, Unit] = Stream.resource(Blocker[IO]).flatMap {
      blocker =>
        tweets
          .through(text.utf8Encode)
          .through(stdout(blocker))
    }

    printer.compile.drain.as(ExitCode.Success)
  }
}
