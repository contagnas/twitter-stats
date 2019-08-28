package twitterstats

import java.time.temporal.ChronoUnit
import java.time.{Instant, ZonedDateTime}

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

class TwitterStatsApi(
  tweetSignal: Signal[IO, WindowedSum[TweetStats]],
  clock: Clock[IO]
) extends Http4sDsl[IO] {

  case class Seconds(get: Duration)

  implicit val secondsQueryParamMatcher: QueryParamDecoder[Seconds] =
    QueryParamDecoder[Long].map(s => Seconds(s.seconds))

  object SecondsQueryParamDecoderMatcher extends QueryParamDecoderMatcher[Seconds]("seconds")
  object ListLimitQueryParam extends OptionalQueryParamDecoderMatcher[Int]("list_limit")

  implicit val hostKey: Encoder[Host] = Encoder[String].contramap(_.value)

  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "twitterStats" :? SecondsQueryParamDecoderMatcher(seconds) +& ListLimitQueryParam(limit) =>
      Ok {
        for {
          tweet <- tweetSignal.get
          statsResponse <- generateResponse(tweet, limit, seconds.get, clock)
        } yield statsResponse
      }
  }

  def generateResponse(
    tweetStats: WindowedSum[TweetStats],
    inputLimit: Option[Int],
    duration: Duration, clock: Clock[IO]
  ): IO[TweetStatsResponse] = clock.realTime(MILLISECONDS)
    .map(Instant.ofEpochMilli)
    .map(m => ZonedDateTime.ofInstant(m, java.time.Clock.systemDefaultZone.getZone))
    .map { windowEnd =>
      val window = tweetStats.shrinkWindowTo(duration, windowEnd)
      val stats = window.value
      val limit = inputLimit.getOrElse(100)

      def sortCount[A](countByName: Map[A, Int]) = countByName.toList
        .map { case (name, count) => Count(name, count) }
        .sortBy(-1 * _.count)
        .take(limit)

      TweetStatsResponse(
        secondsElapsed = ChronoUnit.SECONDS.between(window.firstTimestamp, window.lastTimestamp),
        count = stats.count,
        hashtags = sortCount(stats.hashtags),
        domains = sortCount(stats.domains),
        tweetsWithUrls = stats.tweetsWithUrls,
        percentWithUrls = stats.tweetsWithUrls.toDouble / stats.count,
        tweetsWithPhotos = stats.tweetsWithPhotos,
        percentWithPhotos = stats.tweetsWithPhotos.toDouble / stats.count,
        mediaTypes = sortCount(stats.mediaTypes),
        mediaDomains = sortCount(stats.mediaDomains),
        emojis = sortCount(stats.emojis),
        tweetsWithEmojis = stats.tweetsWithEmojis,
        percentWithEmojis = stats.tweetsWithEmojis.toDouble / stats.count,
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
  hashtags: List[Count[String]],
  domains: List[Count[Host]],
  tweetsWithUrls: Int,
  percentWithUrls: Double,
  tweetsWithPhotos: Int,
  percentWithPhotos: Double,
  mediaTypes: List[Count[TweetMedia]],
  mediaDomains: List[Count[Host]],
  emojis: List[Count[String]],
  tweetsWithEmojis: Int,
  percentWithEmojis: Double,
  firstTweetInWindow: ZonedDateTime,
  lastTweetInWindow: ZonedDateTime,
)