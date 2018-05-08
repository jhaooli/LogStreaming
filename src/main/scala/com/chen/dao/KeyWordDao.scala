package com.chen.dao

import com.chen.entity.KeyWordCount
import com.chen.utils.HBaseUtil
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object KeyWordDao {

  val tableName = "log_keywordcount"
  val cf = "info"
  val qualifer = "search_count"

  def save(list : ListBuffer[KeyWordCount]) = {
    val table = HBaseUtil.getInstance().getTable(tableName)
    for(element <- list) {
      table.incrementColumnValue(element.keyword.getBytes(),
        cf.getBytes(), qualifer.getBytes(), element.count)
    }
  }

  def queryByKey(keyWord: String) = {
    val table = HBaseUtil.getInstance().getTable(tableName)
    val get = new Get(keyWord.getBytes())
    val value = table.get(get).getValue(cf.getBytes(),qualifer.getBytes())
    if(value == null) {
      0l
    }
    Bytes.toLong(value)
  }
}
