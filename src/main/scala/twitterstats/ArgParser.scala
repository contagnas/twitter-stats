package twitterstats

import org.http4s.implicits._
import org.http4s.Uri
import org.http4s.client.oauth1.{Consumer, Token}

import scala.annotation.tailrec

object ArgParser {
  case class Config(
    authToken: Token,
    authConsumer: Consumer,
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
        authConsumer = Consumer(apiKey.get, apiSecretKey.get),
        authToken = Token(accessToken.get, accessTokenSecret.get),
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
}
