package twitterstats

import cats.implicits._
import cats.{Monoid, Show, derived}
import com.vdurmont.emoji.EmojiParser
import org.http4s.Uri.Host
import twitterstats.Twitter.{Tweet, TweetMedia}

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
  emoji: Map[String, Int]
)

object TweetStats {
  private def count[A](as: Iterable[A]): Map[A, Int] = as.groupBy(identity)
    .map { case (k, vs) => k -> vs.size }

  def of(tweet: Tweet): TweetStats = {
    val emojis = EmojiParser.extractEmojis(tweet.text).asScala
    TweetStats(
      count = 1,
      hashtags = count(tweet.hashtags),
      tweetsWithUrls = if (tweet.urls.nonEmpty) 1 else 0,
      tweetsWithPhotos = if (tweet.mediaUrls.exists(_.mediaType == TweetMedia.Photo)) 1 else 0,
      tweetsWithEmojis = if (emojis.nonEmpty) 1 else 0,
      domains = count(tweet.urls.flatMap(_.host)),
      mediaTypes = count(tweet.mediaUrls.map(_.mediaType)),
      mediaDomains = count(tweet.mediaUrls.flatMap(_.url.host)),
      emoji = count(emojis)
    )
  }

  implicit val monoidStats: Monoid[TweetStats] = derived.semi.monoid

  implicit val showHost: Show[Host] = h => h.toString

  implicit def showCounts[K: Show]: Show[Map[K, Int]] = (t: Map[K, Int]) => {
    val s = t.toList
      .sortBy { case (_, v) => -v }
      .take(5)
      .map { case (k, v) => Show[K].show(k) + " -> " + v }
      .mkString(",\n  ")
    s"\n  $s\n"
  }

  implicit val showStats: Show[TweetStats] = t =>
    s"""
       |count = ${t.count.show}
       |tweetsWithUrls = ${t.tweetsWithUrls.show}
       |tweetsWithPhotos = ${t.tweetsWithPhotos.show}
       |tweetsWithEmojis = ${t.tweetsWithEmojis.show}
       |hashtags = {${t.hashtags.show}}
       |domains = {${t.domains.show}}
       |mediaTypes = {${t.mediaTypes.show}}
       |mediaDomains = {${t.mediaDomains.show}}
       |emoji = {${t.emoji.show}}
      """.stripMargin

}
