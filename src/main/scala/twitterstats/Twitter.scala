package twitterstats

import cats.effect.ConcurrentEffect
import fs2.Stream
import io.circe.Json
import io.circe.jawn.CirceSupportParser
import jawnfs2._
import org.http4s.Request
import org.http4s.client.blaze.BlazeClientBuilder
import org.typelevel.jawn.RawFacade

import scala.concurrent.ExecutionContext.global

class Twitter[F[_]: ConcurrentEffect] {
  private implicit val circeSupportParser: RawFacade[Json] =
    new CirceSupportParser(None, false).facade

  def tweetStream(request: F[Request[F]]): Stream[F, Json] = for {
    client <- BlazeClientBuilder[F](global).stream
    req <- Stream.eval(request)
    rawTweet <- client.stream(req)
    tweet <- rawTweet.body.chunks.parseJsonStream
  } yield tweet
}
