

import java.io._
import java.math.BigInteger
import com.moilioncircle.redis.replicator.util.CRC64

import scala.collection.immutable.{Map, Set}


case class RedisHash(values: Map[String, String])

case class RedisZSet(values: Map[String, Double])

case class RedisSet(values: Set[String])
case class RedisList(values: List[String])



class RDBCreator private(private val out: OutputStream, private val version: Int, private val database: Int) {

  private var crc = 0L

  private val RDB_TYPE_STRING: Byte = 0x00
  private val RDB_TYPE_LIST: Byte = 0x01
  private val RDB_TYPE_SET: Byte = 0x02
  private val RDB_TYPE_ZSET: Byte = 0x03
  private val RDB_TYPE_HASH: Byte = 0x04
  private val RDB_TYPE_ZSET_2: Byte = 0x05
  private val RDB_TYPE_HASH_ZIPMAP: Byte = 0x09
  private val RDB_TYPE_LIST_ZIPLIST: Byte = 0x0a
  private val RDB_TYPE_SET_INTSET: Byte = 0x0b
  private val RDB_TYPE_ZSET_ZIPLIST: Byte = 0x0c
  private val RDB_TYPE_HASH_ZIPLIST: Byte = 0x0d
  private val RDB_TYPE_LIST_QUICKLIST: Byte = 0x0e
  private val RDB_OPCODE_EOF: Byte = 0xff.toByte
  private val RDB_OPCODE_SELECTDB: Byte = 0xfe.toByte


  private def convertString(string: String): List[Byte] = {
    val length = encodeLength(string.length)
    length ++ string.map(_.toByte).toList
  }

  private def encodeLength(length: Int): List[Byte] = {
    if (length <= 63)
      List(BigInt(length).toByteArray.last)
    else if (length <= 16383)
      List((64 + length / 256).toByte, (length % 256).toByte)
    else
      0x80.toByte :: BigInt(length).toByteArray.toList
  }

  private def createHeader(version: Int, database: Int): List[Byte] = {
    val dbCode = List(RDB_OPCODE_SELECTDB, database.toByte)
    val versionText = version.toString.reverse.padTo(4, '0').reverse
    ("REDIS" + versionText).getBytes.toList ++ dbCode
  }

  private def convertPairs(map: List[(String, String)]) =
    map.flatMap(pair => convertString(pair._1) ++ convertString(pair._2))


  def write(key: String, value: Any): Unit = {
    val keyBytes = convertString(key)
    val pair: List[Byte] = value match {
      case text: String =>
        RDB_TYPE_STRING :: keyBytes ++ convertString(text)
      case RedisList(list)=>
        RDB_TYPE_LIST :: keyBytes ++ encodeLength(list.size) ++ list.flatMap(convertString)
      case RedisSet(set) =>
        RDB_TYPE_SET :: keyBytes ++ encodeLength(set.size) ++ set.toList.flatMap(convertString)
      case RedisZSet(values) =>
        RDB_TYPE_ZSET :: keyBytes ++ encodeLength(values.size) ++ convertPairs(values.toList.map(pair => (pair._1, pair._2.toString)))
      case RedisHash(map) =>
        RDB_TYPE_HASH :: keyBytes ++ encodeLength(map.size) ++ convertPairs(map.toList)
    }
    updateCRC(pair)
    write(pair)
  }

  private def updateCRC(bytes: List[Byte]): Unit = {
    crc = CRC64.crc64(bytes.toArray, crc)
  }

  private def write(bytes: List[Byte]): Unit = {
    for (byte <- bytes)
      out.write(byte)
  }

  def open(): Unit = {
    val header = createHeader(version, database)
    crc = CRC64.crc64(header.toArray)
    write(header)
  }


  def close(): Unit = {
    write(List(RDB_OPCODE_EOF))
    updateCRC(List(RDB_OPCODE_EOF))
    val footer = BigInteger
      .valueOf(crc)
      .toByteArray
      .toList
      .reverse
    write(footer)
    out.close()
  }
}

object RDBCreator {
  def apply(out: OutputStream, version: Int = 7, database: Int = 0) = new RDBCreator(out, version, database)
}
