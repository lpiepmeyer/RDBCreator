# A Creator For Redis RDB Files

All data of the in memory database system Redis can be [dumped to a file](https://redis.io/docs/management/persistence/). The file format of rdb files is [documented](https://rdb.fnordig.de/file_format.html#redis-rdb-file-format). 
The type `RDBCreator` of this repo can be used to create rdb files. This can be useful for bulk loads. 


## Installation

* Install the Scala Build Tool [(sbt)](https://www.scala-sbt.org/1.x/docs/Setup.html)

* Download this repository and change to the directory with this README file.

## Usage

Call the methods `open`, `write` and `close` methods as in the following example:

```
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
```

Executing `sbt run` creates the file `dump.rdb`. 
On macOS the following commands can be used to populate the Redis database:

```
brew services stop redis
cp ./data/dump.rdb <redis data folder>
brew services start redis
```
On other operating systems other commands are used to bounce Redis. 
Start the Redis command line tool to observe the results:

```
redis-cli
127.0.0.1:6379> keys *
1) "b"
2) "d"
3) "c"
4) "a"
5) "e"
127.0.0.1:6379> get a
"0"
127.0.0.1:6379> lrange b 0 -1
1) "1"
2) "2"
3) "3"
127.0.0.1:6379> smembers c
1) "4"
2) "5"
3) "6"
127.0.0.1:6379> hgetall d
1) "x"
2) "y"
3) "u"
4) "v"
127.0.0.1:6379> zrange e 0 -1 withscores
1) "u"
2) "11"
3) "x"
4) "47"
```


