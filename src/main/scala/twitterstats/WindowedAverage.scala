package twitterstats

import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

import cats.Semigroup
import cats.implicits._
import shapeless._

import scala.concurrent.duration._

/*
 * Given a stream timestamped data, approximate how many of them occur in the
 * last second, minute, hour, etc.
 *
 * This implementation assumes that existing average is from a uniform
 * distribution which probably isn't great for a stream of tweets, but it is very
 * simple and should be fine over relatively small time horizons.
 */
case class WindowedAverage[A: Decayable: Semigroup](
  value: A,
  lastTimeStamp: ZonedDateTime,
  window: Duration
) {
  def addValue(
    addedValue: A,
    timestamp: ZonedDateTime,
  ): WindowedAverage[A] = {
    val newValue = if (timestamp.isAfter(lastTimeStamp)) {
      val timeElapsed: Long = lastTimeStamp.until(timestamp, ChronoUnit.MILLIS)
      val decay: Double = math.min(math.max(timeElapsed.toDouble, 0.0) / this.window.toMillis, 1)
      Decayable[A].decay(value, decay) |+| addedValue
    } else {
      // Receiving timestamps out of order is weird, but put the value into the bucket if it falls in the bucket
      if (!timestamp.isBefore(lastTimeStamp.minusSeconds(this.window.toSeconds))) {
        value |+| addedValue
      } else {
        value
      }
    }

    this.copy(
      value = newValue,
      lastTimeStamp = timestamp
    )
  }
}

trait Decayable[A] {
  def decay(value: A, decayAmount: Double): A
}

object Decayable {
  def apply[A: Decayable]: Decayable[A] = implicitly[Decayable[A]]

  def instance[A](func: (A, Double) => A): Decayable[A] =
    (value: A, decayAmount: Double) => func(value, decayAmount)

  implicit val decayableDouble: Decayable[Double] =
    (value: Double, decayAmount: Double) => value * (1 - decayAmount)

  implicit val decayableInt: Decayable[Int] =
    (value: Int, decayAmount: Double) =>
      Decayable[Double].decay(value.toDouble, decayAmount).ceil.toInt

  implicit def decayableMap[K, V: Decayable]: Decayable[Map[K, V]] =
    (value: Map[K, V], decayAmount: Double) => value.map {
      case (k, v) => k -> Decayable[V].decay(v, decayAmount)
    }
}

object Decay {
  def addValue(
    nextValue: Int,
    timestamp: ZonedDateTime,
    runningAverage: WindowedAverage[Int]
  ): WindowedAverage[Int] = {
    val timeElapsed: Long = runningAverage.lastTimeStamp.until(timestamp, ChronoUnit.SECONDS)
    val decay: Double = math.max(timeElapsed.toDouble, 0.0) / runningAverage.window.toSeconds
    val newValue = runningAverage.value * decay + nextValue
    runningAverage.copy(
      value = newValue.toInt,
      lastTimeStamp = timestamp
    )
  }
}

/**
  * Derive a Decayable instance for a product from the Decayable instances of its members
  *
  * This is based on derivations in the kittens project, e.g.:
  * https://github.com/typelevel/kittens/blob/master/core/src/main/scala/cats/derived/semigroup.scala
  */
object DeriveDecayable {
  def decayable[A](implicit ev: Lazy[MkDecayable[A]]): Decayable[A] = ev.value
}

trait MkDecayable[A] extends Decayable[A]

object MkDecayable extends MkDecayableDerivation {
  def apply[A](implicit ev: MkDecayable[A]): MkDecayable[A] = ev
}

abstract class MkDecayableDerivation {
  implicit val mkDecayableHNil: MkDecayable[HNil] = (_, _) => HNil

  implicit def mkDecayableGeneric[A, R](implicit A: Generic.Aux[A, R], R: Lazy[MkDecayable[R]]): MkDecayable[A] =
    instance((x, y) => A.from(R.value.decay(A.to(x), y)))

  implicit def hlistDecayable[H, T <: HList](
    implicit H: Decayable[H] OrElse MkDecayable[H], T: MkDecayable[T]
  ): MkDecayable[H :: T] = instance { case (hx :: tx, decay) => H.unify.decay(hx, decay) :: T.decay(tx, decay) }

  private def instance[A](f: (A, Double) => A): MkDecayable[A] =
    (x: A, y: Double) => f(x, y)
}
