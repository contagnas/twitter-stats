package twitterstats

import java.time.ZonedDateTime

import cats.effect._
import cats.implicits._
import cats.{Monoid, Show, derived}
import com.vdurmont.emoji.EmojiParser
import fs2.Stream
import fs2.io.stdoutLines
import io.circe.Json
import io.circe.jawn.CirceSupportParser
import org.http4s.Uri.Host
import org.http4s._
import org.http4s.client.oauth1
import org.typelevel.jawn.RawFacade
import twitterstats.Twitter.{Tweet, TweetMedia}

import scala.concurrent.duration._
import scala.collection.JavaConverters._

private object Main extends IOApp {

  implicit val circeSupportParser: RawFacade[Json] = new CirceSupportParser(None, false).facade

  case class Stats(
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

  def count[A](as: Iterable[A]): Map[A, Int] = as.groupBy(identity)
    .map { case (k, vs) => k -> vs.size }

  def tweetToStats(tweet: Tweet): Stats = Stats(
    count = 1,
    hashtags = count(tweet.hashtags),
    tweetsWithUrls = if (tweet.urls.nonEmpty) 1 else 0,
    tweetsWithPhotos = if (tweet.mediaUrls.exists(_.mediaType == TweetMedia.Photo)) 1 else 0,
    tweetsWithEmojis = if (EmojiParser.extractEmojis(tweet.text).isEmpty) 0 else 1,
    domains = count(tweet.urls.flatMap(_.host)),
    mediaTypes = count(tweet.mediaUrls.map(_.mediaType)),
    mediaDomains = count(tweet.mediaUrls.flatMap(_.url.host)),
    emoji = count(EmojiParser.extractEmojis(tweet.text).asScala)
  )

  implicit val monoidStats: Monoid[Stats] = derived.semi.monoid

  implicit val showHost: Show[Host] = h => h.toString

  implicit def showCounts[K: Show]: Show[Map[K, Int]] = (t: Map[K, Int]) => {
    val s = t.toList
      .sortBy { case (_, v) => -v }
      .take(5)
      .map { case (k, v) => Show[K].show(k) + " -> " + v }
      .mkString(",\n  ")
    s"\n  $s\n"
  }

  implicit val showStats: Show[Stats] = t =>
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
          .scan(WindowedSum.of(Monoid[Stats].empty, ZonedDateTime.now, 1.hour))(
            (avg, tweet) => avg.addValue(tweetToStats(tweet), tweet.timestamp))
          .map(_.valueForLast(30.seconds))
          .through(stdoutLines(blocker))
          .take(1000)
    }

    printer.compile.drain.as(ExitCode.Success)
  }
}
