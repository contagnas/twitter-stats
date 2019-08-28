package twitterstats

import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

import cats.effect._
import fs2.concurrent.Signal
import io.circe.Encoder
import io.circe.generic.auto._
import org.http4s.Uri.Host
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpRoutes, _}
import twitterstats.Twitter.TweetMedia

import scala.concurrent.duration._

class TwitterStatsApi(tweetSignal: Signal[IO, WindowedSum[TweetStats]]) extends Http4sDsl[IO] {

  case class Seconds(get: Duration)

  implicit val secondsQueryParamMatcher: QueryParamDecoder[Seconds] =
    QueryParamDecoder[Long].map(s => Seconds(s.seconds))

  object SecondsQueryParamDecoderMatcher extends QueryParamDecoderMatcher[Seconds]("seconds")
  object ListLimitQueryParam extends OptionalQueryParamDecoderMatcher[Int]("list_limit")

  implicit val hostKey: Encoder[Host] = Encoder[String].contramap(_.value)

  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "twitterStats" :? SecondsQueryParamDecoderMatcher(seconds) +& ListLimitQueryParam(limit) =>
      Ok {
        tweetSignal.get
          .map(tweetStats => generateResponse(tweetStats, limit, seconds.get))
      }
  }

  def generateResponse(
    tweetStats: WindowedSum[TweetStats],
    inputLimit: Option[Int],
    duration: Duration
  ): TweetStatsResponse = {
      val window = tweetStats.shrinkWindowTo(duration)
      val stats = window.value
      val limit = inputLimit.getOrElse(100)

      def sortCount[A](countByName: Map[A, Int]) = countByName.toList
        .map { case (name, count) => Count(name, count) }
        .sortBy(-1 * _.count)
        .take(limit)

      def percent(value: Int): Double = 100.0 * value / math.max(stats.count, 1)

      TweetStatsResponse(
        secondsElapsed = ChronoUnit.SECONDS.between(window.firstTimestamp, window.lastTimestamp),
        count = stats.count,
        hashtags = sortCount(stats.hashtags),
        domains = sortCount(stats.domains),
        tweetsWithUrls = stats.tweetsWithUrls,
        percentWithUrls = percent(stats.tweetsWithUrls),
        tweetsWithPhotos = stats.tweetsWithPhotos,
        percentWithPhotos = percent(stats.tweetsWithPhotos),
        mediaTypes = sortCount(stats.mediaTypes),
        mediaDomains = sortCount(stats.mediaDomains),
        emojis = sortCount(stats.emojis),
        tweetsWithEmojis = stats.tweetsWithEmojis,
        percentWithEmojis = percent(stats.tweetsWithEmojis),
        firstTweetInWindow = window.firstTimestamp,
        lastTweetInWindow = window.lastTimestamp,
      )
    }
}

// See src/main/swagger/swagger.yaml
case class Count[A](name: A, count: Int)
case class TweetStatsResponse(
  secondsElapsed: Long,
  count: Int,
  firstTweetInWindow: ZonedDateTime,
  lastTweetInWindow: ZonedDateTime,
  tweetsWithUrls: Int,
  percentWithUrls: Double,
  tweetsWithPhotos: Int,
  percentWithPhotos: Double,
  tweetsWithEmojis: Int,
  percentWithEmojis: Double,
  hashtags: List[Count[String]],
  domains: List[Count[Host]],
  mediaTypes: List[Count[TweetMedia]],
  mediaDomains: List[Count[Host]],
  emojis: List[Count[String]],
)
