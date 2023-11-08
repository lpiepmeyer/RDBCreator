import java.io.ByteArrayOutputStream
import scala.collection.immutable.{Map, Set}

class RDBCreatorTest extends org.scalatest.funsuite.AnyFunSuiteLike {
  test("empty rdb") {
    val out = new ByteArrayOutputStream()
    val rdb = RDBCreator(out)
    rdb.open()
    rdb.close()
    val actual = out.toByteArray.toList
    val expected= List(82, 69, 68, 73, 83, 48, 48, 48, 55, -2, 0, -1, 99, -97, -12, -15, -21, 111, -96, -39)
    assert(actual == expected)
  }

  private def check(key: String, value:Any, expected: List[Byte]): Unit = {
    val out = new ByteArrayOutputStream()
    val rdb = RDBCreator(out)
    rdb.open()
    rdb.write(key,value)
    rdb.close()
    val actual = out.toByteArray.toList
    assert(actual == expected)
  }

  test("string pair") {
    val expected = List[Byte](82, 69, 68, 73, 83, 48, 48, 48, 55, -2, 0, 0, 1, 97, 1, 48, -1, 8, 100, 55, 85, -26, -6, -60, 77)
    check("a","0",expected)
  }

  test("redis list") {
    val expected = List[Byte](82, 69, 68, 73, 83, 48, 48, 48, 55, -2, 0, 1, 1, 97, 3, 1, 49, 1, 50, 1, 51, -1, 65, -35, 86, 25, -103, -93, 67, 120)
    check("a", RedisList("1","2","3"), expected)
  }

  test("redis set") {
    val expected = List[Byte](82, 69, 68, 73, 83, 48, 48, 48, 55, -2, 0, 2, 1, 97, 3, 1, 52, 1, 53, 1, 54, -1, -26, -39, -123, 21, 81, 36, 75, 40)
    check("a", RedisSet("4", "5", "6"), expected)
  }

  test("redis hash") {
    val expected = List[Byte](82, 69, 68, 73, 83, 48, 48, 48, 55, -2, 0, 4, 1, 97, 2, 1, 120, 1, 121, 1, 117, 1, 118, -1, -30, 0, 51, 124, 31, 5, 44, 34)
    check("a", RedisHash("x" -> "y", "u" -> "v"), expected)
  }


  test("redis sorted set") {
    val expected = List[Byte](82, 69, 68, 73, 83, 48, 48, 48, 55, -2, 0, 3, 1, 97, 2, 1, 120, 4, 52, 55, 46, 48, 1, 117, 4, 49, 49, 46, 48, -1, 120, -63, 76, -59, -75, -102, -54, -89)
    check("a", RedisZSet("x" -> 47.0, "u" -> 11.0), expected)
  }
}
