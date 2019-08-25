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

  // value close enough to 0 to evict from the window
  def gtEpsilon(a: A): Boolean
}

object Decayable {
  def apply[A: Decayable]: Decayable[A] = implicitly[Decayable[A]]

  def instance[A](func: (A, Double) => A, gtEpsi: A => Boolean): Decayable[A] = new Decayable[A] {
    override def decay(value: A, decayAmount: Double): A = func(value, decayAmount)
    override def gtEpsilon(a: A): Boolean = gtEpsi(a)
  }

  implicit val decayableDouble: Decayable[Double] = new Decayable[Double] {
    override def decay(value: Double, decayAmount: Double): Double =
      value * (1 - decayAmount)

    override def gtEpsilon(a: Double): Boolean = a > 1E-4
  }

  implicit val decayableInt: Decayable[Int] = new Decayable[Int] {
    override def decay(value: Int, decayAmount: Double): Int =
      Decayable[Double].decay(value.toDouble, decayAmount).ceil.toInt

    override def gtEpsilon(a: Int): Boolean = a > 0
  }

  implicit def decayableMap[K, V: Decayable]: Decayable[Map[K, V]] = new Decayable[Map[K, V]] {
    override def decay(value: Map[K, V], decayAmount: Double): Map[K, V] =
      value
        .map { case (k, v) => k -> Decayable[V].decay(v, decayAmount) }
        .filter { case (k, v) => Decayable[V].gtEpsilon(v) }

    override def gtEpsilon(a: Map[K, V]): Boolean = a.nonEmpty
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
  implicit val mkDecayableHNil: MkDecayable[HNil] = new MkDecayable[HNil] {
    override def decay(value: HNil, decayAmount: Double): HNil = value
    override def gtEpsilon(a: HNil): Boolean = false
  }

  implicit def mkDecayableGeneric[A, R](implicit A: Generic.Aux[A, R], R: Lazy[MkDecayable[R]]): MkDecayable[A] =
    new MkDecayable[A] {
      override def decay(value: A, decayAmount: Double): A =
        A.from(R.value.decay(A.to(value), decayAmount))

      override def gtEpsilon(value: A): Boolean =
        R.value.gtEpsilon(A.to(value))
    }

  implicit def hlistDecayable[H, T <: HList](
    implicit H: Decayable[H] OrElse MkDecayable[H], T: MkDecayable[T]
  ): MkDecayable[H :: T] =  new MkDecayable[H :: T] {
    override def decay(value: H :: T, decayAmount: Double): H :: T =
      value match {
        case hx :: tx => H.unify.decay(hx, decayAmount) :: T.decay(tx, decayAmount)
      }
    override def gtEpsilon(a: H :: T): Boolean = a match {
      case hx :: tx => H.unify.gtEpsilon(hx) && T.gtEpsilon(tx)
    }
  }
}
