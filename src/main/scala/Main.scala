import java.io.FileOutputStream

object Main extends App{
  val out = new FileOutputStream("./data/dump.rdb")
  val rdb = RDBCreator(out)
  rdb.open()
  Map(
    "a" -> "0",
    "b" -> RedisList(List("1", "2", "3")),
    "c" -> RedisSet(Set("4", "5", "6")),
    "d" -> RedisHash(Map("x" -> "y", "u" -> "v")),
    "e" -> RedisZSet(Map("x" -> 47.0, "u" -> 11.0))
  ).foreach { pair =>
    rdb.write(pair._1, pair._2)
  }
  rdb.close()
}
