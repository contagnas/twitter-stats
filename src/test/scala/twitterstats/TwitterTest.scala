package twitterstats

import io.circe.Json
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

class TwitterTest extends FlatSpec with Matchers {
  def getJson(filename: String): Json = {
    val lines = Source.fromURL(getClass.getResource(filename))
    val jsonString = try lines.mkString finally lines.close()
    io.circe.jawn.parse(jsonString).right.get
  }

  "Tweet parser" should "use the full_text if it exists" in {
    val extendedTweet = Twitter.parse(getJson("/extended_tweet.json")).right.get
    extendedTweet.text should be ("モンスト #6周年カウントダウン スタート！｢オーブ毎週50個以上｣当たる抽選にチャレンジした結果、オーブ｢50個｣獲得したよ！チャンスは最大3回！第3週目は9/1 AM3:59まで参加できるから、みんなも受け取ってね！ #モンストプリズン #オーブ毎週50個以上配布 https://t.co/LE7cUBctkQ")
  }

  it should "use the regular text if no full_text exists" in {
    val shortTweet = Twitter.parse(getJson("/short_tweet.json")).right.get
    shortTweet.text should be ("今日帰って眠くなければ喋りぺりすこするよ予告。\nカバー曲特集しよう思ってる。")
  }

  it should "not crash on jsons that aren't tweets" in {
    val notATweet = Twitter.parse(getJson("/delete.json"))
    notATweet.isLeft should be (true)
  }
}
