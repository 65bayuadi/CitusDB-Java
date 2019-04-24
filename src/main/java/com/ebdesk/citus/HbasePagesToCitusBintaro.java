package com.ebdesk.citus;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;
import scala.Byte;

import java.io.IOException;

public class HbasePagesToCitusBintaro {
    public static void main(String[] args) throws IOException, ServiceException {
        SparkSession sparkSession = new SparkSession.Builder().appName("cek").master("local[2]").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.master", "namenode01.hdp03.bt,namenode02.hdp03.bt");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        config.set("timeout", "40000");
        config.set("hbase.zookeeper.quorum", "master.hdp03.bt,namenode01.hdp03.bt,namenode02.hdp03.bt");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set(TableInputFormat.INPUT_TABLE, "fb-pages");

        HBaseAdmin.checkHBaseAvailable(config);

        JavaRDD<Row> result = jsc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
                .map(data -> {
                    Result results = data._2();
                    JSONObject object = new JSONObject(Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("data"))));

                    String id = object.has("id") ? object.get("id").toString() : "n/a";
                    String about = object.has("about") ? object.get("about").toString() : "n/a";
                    String fan_count = object.has("fan_count") ? object.get("fan_count").toString() : "n/a";
                    String talking_about_count = object.has("talking_about_count") ? object.get("talking_about_count").toString() : "n/a";
                    String name = object.has("name") ? object.get("name").toString() : "n/a";
                    String link = object.has("link") ? object.get("link").toString() : "n/a";
                    String category = object.has("category") ? object.get("category").toString() : "n/a";
                    String picture = object.has("picture") ? object.get("picture").toString() : "n/a";
                    String awards = object.has("awards") ? object.get("awards").toString() : "n/a";
                    String verification_status = object.has("verification_status") ? object.get("verification_status").toString() : "n/a";
                    String location = object.has("location") ? object.get("location").toString() : "n/a";
                    String username = object.has("username") ? object.get("username").toString() : "n/a";

                    return RowFactory.create(id, about, fan_count, talking_about_count, name, link, category, picture, awards, verification_status, location, username);
                });


    }
}
