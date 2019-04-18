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
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Properties;

public class HbaseToCitus {
    public static void main(String[] args) throws IOException, ServiceException {
        SparkSession sparkSession = new SparkSession.Builder().appName("HbaseToCitus").master("local[2]").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.master", "namenode01.cluster.ph,namenode02.cluster.ph");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        config.set("timeout", "40000");
        config.set("hbase.zookeeper.quorum", "master.cluster.ph,namenode01.cluster.ph,namenode02.cluster.ph");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set(TableInputFormat.INPUT_TABLE, "ipd-fb-group-member");
//        config.set(TableInputFormat.SCAN_COLUMN_FAMILY, "0");

//        config.set(TableInputFormat.SCAN_TIMERANGE_START, "1547613387152");
//        config.set(TableInputFormat.SCAN_TIMERANGE_END, "1547613420000");

        HBaseAdmin.checkHBaseAvailable(config);

        JavaRDD<Row> result = jsc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
                .map(data -> {
                    JSONObject object = new JSONObject();
                    Result results = data._2();

                    String age = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("age"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("age"))) : "n/a";
                    object.put("age", age);

                    return RowFactory.create(object.get("age"));
                });

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("age", DataTypes.StringType, false)
        });

        Dataset<Row> df = sparkSession.createDataFrame(result, schema);

        Properties properties = new Properties();
        properties.put("user", "postgres");
        properties.put("password", "");

        df.write().mode(SaveMode.Overwrite).option(JDBCOptions.JDBC_DRIVER_CLASS(), "org.postgresql.Driver").jdbc("jdbc:postgresql://192.168.99.95:5432/postgres", "age", properties);


    }
}
