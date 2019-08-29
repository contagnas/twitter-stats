package twitterstats

import java.util.Locale

import cats.{Monoid, derived}
import com.vdurmont.emoji.EmojiParser
import org.http4s.Uri.Host
import twitterstats.Twitter.{Tweet, TweetMedia}
import cats.implicits._

import scala.collection.JavaConverters._

case class TweetStats(
  count: Int,
  hashtags: Map[String, Int],
  tweetsWithUrls: Int,
  tweetsWithPhotos: Int,
  tweetsWithEmojis: Int,
  domains: Map[Host, Int],
  mediaTypes: Map[TweetMedia, Int],
  mediaDomains: Map[Host, Int],
  emojis: Map[String, Int],
  languages: Map[Locale, Int]
)

object TweetStats {
  private def countUnique[A](as: Iterable[A]): Map[A, Int] = as.groupBy(identity)
    .map { case (k, vs) => k -> vs.size }

  def of(tweet: Tweet): TweetStats = {
    val emojis = EmojiParser.extractEmojis(tweet.text).asScala
    TweetStats(
      count = 1,
      hashtags = countUnique(tweet.hashtags),
      tweetsWithUrls = if (tweet.urls.nonEmpty) 1 else 0,
      tweetsWithPhotos = if (tweet.mediaUrls.exists(_.mediaType == TweetMedia.Photo)) 1 else 0,
      tweetsWithEmojis = if (emojis.nonEmpty) 1 else 0,
      domains = countUnique(tweet.urls.flatMap(_.host)),
      mediaTypes = countUnique(tweet.mediaUrls.map(_.mediaType)),
      mediaDomains = countUnique(tweet.mediaUrls.flatMap(_.url.host)),
      emojis = countUnique(emojis),
      languages = tweet.language.map(l => Map(l -> 1)).getOrElse(Map.empty)
    )
  }

  1 |+| 1 // intellij marks cats.implicits as unused, but its needed for Monoid[Map[Locale, Int]]
  implicit val monoidStats: Monoid[TweetStats] = derived.semi.monoid
}
