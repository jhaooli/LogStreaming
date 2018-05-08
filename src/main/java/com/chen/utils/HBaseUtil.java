package com.chen.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HBaseUtil {

    private HBaseAdmin admin = null;
    private Configuration conf = null;

    private HBaseUtil(){
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "master:2181");
       // conf.set("hbase.rootdir", "hdfs://master8020/hbase");
        try{
            admin = new HBaseAdmin(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtil instance = null;

    public static synchronized HBaseUtil getInstance() {
        if (instance == null) {
            return new HBaseUtil();
        }
        return instance;
    }

    /**
     * get table
     * @param tableName
     * @return
     */
    public HTable getTable(String tableName) {
        HTable table = null;
        try {
            table = new HTable(conf, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public Map<String, Long> query(String tableName, String condition) {
        Map<String, Long> map = new HashMap<>();
        HTable table = getTable(tableName);
        String cf = "info";
        String qualifier = "search_count";
        Scan scan = new Scan();
        Filter filter = new PrefixFilter(Bytes.toBytes(condition));
        scan.setFilter(filter);
        ResultScanner rs = null;
        try {
            rs = table.getScanner(scan);
            for (Result result : rs) {
                String keyword = Bytes.toString(result.getRow());
                long count = Bytes.toLong(result.getValue(cf.getBytes(), qualifier.getBytes()));
                map.put(keyword, count);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }
}
