

import java.io._
import java.math.BigInteger
import com.moilioncircle.redis.replicator.util.CRC64

// https://github.com/ae-foster/rdbgenerate/blob/master/rdbgenerate/rdbgen.py

case class Hash(values: Map[String,String])
case class SortedSet(values: Map[String,Double])


object RDBCreator {

  private val RDB_TYPE_STRING : Byte = 0x00
  private val RDB_TYPE_LIST : Byte= 0x01
  private val RDB_TYPE_SET: Byte = 0x02
  private val RDB_TYPE_ZSET: Byte = 0x03
  private val RDB_TYPE_HASH: Byte = 0x04
  private val RDB_TYPE_ZSET_2: Byte = 0x05
  private val RDB_TYPE_HASH_ZIPMAP : Byte= 0x09
  private val RDB_TYPE_LIST_ZIPLIST : Byte= 0x0a
  private val RDB_TYPE_SET_INTSET: Byte = 0x0b
  private val RDB_TYPE_ZSET_ZIPLIST : Byte= 0x0c
  private val RDB_TYPE_HASH_ZIPLIST : Byte= 0x0d
  private val RDB_TYPE_LIST_QUICKLIST: Byte = 0x0e
  private val RDB_OPCODE_EOF : Byte = 0xff.toByte
  private val RDB_OPCODE_SELECTDB : Byte = 0xfe.toByte


  private def convertString(string: String) :List[Byte]= {
    val length = encodeLength(string.length)
     length ++ string.map(_.toByte).toList
  }

  private def encodeLength(length:Int):List[Byte] ={
    if (length <= 63)
       List(BigInt(length).toByteArray.last)
    else if (length <= 16383)
       List((64 + length / 256).toByte, (length % 256).toByte)
    else
       0x80.toByte :: BigInt(length).toByteArray.toList
  }

  private def createHeader(version: Int, database:Int):List[Byte]={
    val dbCode=List(RDB_OPCODE_SELECTDB, database.toByte)
    val versionText = version.toString.reverse.padTo(4, '0').reverse
    ("REDIS" + versionText).getBytes.toList ++ dbCode
  }

  private  def createFooter(data:List[Byte]):List[Byte]={
    val crc=CRC64.crc64(data.toArray)
    val result=BigInteger
      .valueOf(crc)
      .toByteArray
      .toList
      .reverse
    result

  }

  private def convertPairs(map:List[(String,String)])=
    map.flatMap(pair => convertString(pair._1) ++ convertString(pair._2))

  private  def convertValue(pair: (String, Any)): List[Byte] = {
    val key = convertString(pair._1)
    pair._2 match {
      case text: String =>
        RDB_TYPE_STRING :: key ++ convertString(text)
      case list: List[String] =>
         RDB_TYPE_LIST :: key ++ encodeLength(list.size) ++ list.flatMap(convertString)
      case set: Set[String] =>
        RDB_TYPE_SET   :: key ++ encodeLength(set.size)  ++ set.toList.flatMap(convertString)
      case SortedSet(values)=>
        RDB_TYPE_ZSET :: key ++ encodeLength(values.size) ++ convertPairs(values.toList.map(pair=>(pair._1, pair._2.toString)))
      case Hash(map) =>
        RDB_TYPE_HASH :: key ++ encodeLength(map.size) ++ convertPairs(map.toList)

    }
  }

  private  def convertMap(map: Map[String, Any])=
    map.flatMap(pair=>convertValue(pair)).toList

  private  def createContent(start:List[Byte],data:List[Byte]):List[Byte]= {
     (start ++ data) :+  RDB_OPCODE_EOF
  }

  def apply(version:Int, database: Int, data:Map[String,Any]):List[Byte]={
    val bytes = convertMap(data)

    val start = createHeader(version, database)
    val content = createContent(start, bytes)
    val footer = createFooter(content)
    val result = content ++ footer
    result
  }

  def main(args: Array[String]): Unit = {
    val data = Map(
      "a" -> "0",
      "b" -> List("1", "2", "3"),
      "c" -> Set("4", "5", "6"),
      "d" -> Hash(Map("x" -> "y", "u" -> "v")),
      "e" -> SortedSet(Map("x" -> 47.0, "u" -> 11.0))
    )
    val rdb = RDBCreator(7, 0, data)

    val out = new FileOutputStream("./data/dump.rdb")
    for (c <- rdb)
      out.write(c)
    out.close()
  }
}