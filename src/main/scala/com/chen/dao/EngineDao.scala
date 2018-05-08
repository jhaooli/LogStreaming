package com.chen.dao

import com.chen.entity.{EngineCount, KeyWordCount}
import com.chen.utils.HBaseUtil
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object EngineDao {

  val tableName = "log_engine"
  val cf = "info"
  val qualifer = "engine_count"

  def save(list : ListBuffer[EngineCount]) = {
    val table = HBaseUtil.getInstance().getTable(tableName)
    for(element <- list) {
      table.incrementColumnValue(element.engine.getBytes(),
        cf.getBytes(), qualifer.getBytes(), element.count)
    }
  }

  def queryByKey(engine: String) = {
    val table = HBaseUtil.getInstance().getTable(tableName)
    val get = new Get(engine.getBytes())
    val value = table.get(get).getValue(cf.getBytes(),qualifer.getBytes())
    if(value == null) {
      0l
    }
    Bytes.toLong(value)
  }
}
