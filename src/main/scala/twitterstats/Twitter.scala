package twitterstats

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

import cats.effect.ConcurrentEffect
import cats.implicits._
import fs2.Stream
import io.circe.generic.AutoDerivation
import io.circe.jawn.CirceSupportParser
import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import jawnfs2._
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.{Request, Uri}
import org.typelevel.jawn.RawFacade

import scala.concurrent.ExecutionContext.global

class Twitter[F[_]: ConcurrentEffect] {
  private implicit val circeSupportParser: RawFacade[Json] =
    new CirceSupportParser(None, false).facade

  def tweetStream(request: F[Request[F]]): Stream[F, Json] = for {
    client <- BlazeClientBuilder[F](global).stream
    tweetReq <- Stream.eval(request)
    tweetRes <- client.stream(tweetReq)
    tweetJson <- tweetRes.body.chunks.parseJsonStream.filter(!_.asString.contains("delete"))
  } yield tweetJson
}

object Twitter extends AutoDerivation {
  case class Tweet(
    timestamp: ZonedDateTime,
    text: String,
    hashtags: List[String],
    urls: List[Uri],
    mediaUrls: List[MediaUrl],
    language: Option[Locale]
  )

  sealed trait TweetMedia

  object TweetMedia {
    case object Photo extends TweetMedia
    case object Video extends TweetMedia
    case object AnimatedGif extends TweetMedia

    implicit val mediaTypeDecoder: Decoder[TweetMedia] =
      Decoder[String].emap {
        case "photo" => Right(Photo)
        case "video" => Right(Video)
        case "animated_gif" => Right(AnimatedGif)
        case wtf => Left(s"Unrecognized media type: $wtf")
      }

    implicit val mediaTypeEncoder: Encoder[TweetMedia] = Encoder[String].contramap(_.toString)
  }

  case class MediaUrl(
    url: Uri,
    mediaType: TweetMedia
  )

  import DecodingTarget._
  def parse(json: Json): Either[DecodingFailure, Tweet] = {
    /**
      * Twitter puts the real text and entities for long tweets in an "extended" section.
      * Prefer to use that, but if it doesn't exist (because the tweet was short enough to not need it),
      * use the normal field.
      *
      * See https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/extended-entities-object
      *
      * @param rawTweet The RawTweet object
      * @param getExtended a function to extract the extended version of the field
      * @param getRegular a function to extract the normal version of the field
      * @tparam A the type of the field
      * @return getExtended(rawTweet) if it exists, otherwise getRegular(rawTweet)
      */
    def preferExtended[A](rawTweet: RawTweet)(
      getExtended: DecodingTarget.ExtendedTweet => A,
      getRegular: RawTweet => A
    ): A = rawTweet.extended_tweet
      .map(getExtended)
      .getOrElse(getRegular(rawTweet))

    implicit val parseUri: Decoder[Uri] = Decoder[String].emap { s =>
      Uri.fromString(s) match {
        case Left(error) => Left(error.message)
        case Right(uri) => Right(uri)
      }
    }

    json.as[RawTweet].map(
      fromJson => {
        def preferred[A] = preferExtended(fromJson)(_: ExtendedTweet => A, _: RawTweet => A)
        Tweet(
          timestamp = fromJson.created_at,
          text = preferred(_.full_text, _.text),
          hashtags = preferred(_.entities.hashtags, _.entities.hashtags).map(_.text),
          urls = preferred(_.entities.urls, _.entities.urls).map(_.expanded_url),
          mediaUrls = preferred(_.extended_entities, _.extended_entities)
            .map(_.media).sequence.flatten.map(m => MediaUrl(m.media_url, m.`type`)),
          language = fromJson.lang.filterNot(_ == "und").map(Locale.forLanguageTag)
        )
      }
    )
  }

  /**
    * Types for decoding json to the fields we are interested in
    */
  private object DecodingTarget {
    val formatter: DateTimeFormatter =
      DateTimeFormatter.ofPattern("E MMM d HH:mm:ss Z y")
    implicit val localDateTimeDecoder: Decoder[ZonedDateTime] =
      Decoder.decodeString.map(ZonedDateTime.parse(_, formatter))

    case class Hashtag(text: String)
    case class Url(expanded_url: Uri)

    case class Entities(
      hashtags: List[Hashtag],
      urls: List[Url],
    )

    case class ExtendedEntities(
      media: List[Media]
    )

    case class ExtendedTweet(
      full_text: String,
      entities: Entities,
      extended_entities: Option[ExtendedEntities],
    )

    case class Media(
      media_url: Uri,
      `type`: TweetMedia
    )

    case class RawTweet(
      created_at: ZonedDateTime,
      text: String,
      entities: Entities,
      extended_entities: Option[ExtendedEntities],
      extended_tweet: Option[ExtendedTweet],
      lang: Option[String]
    )
  }
}

