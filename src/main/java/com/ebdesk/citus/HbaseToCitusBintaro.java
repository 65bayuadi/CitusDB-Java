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

import java.io.IOException;
import java.util.Properties;

public class HbaseToCitusBintaro {

    public static void main(String[] args) throws IOException, ServiceException {
        SparkSession sparkSession = new SparkSession.Builder().getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.master", "namenode01.hdp03.bt,namenode02.hdp03.bt");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        config.set("timeout", "40000");
        config.set("hbase.zookeeper.quorum", "master.hdp03.bt,namenode01.hdp03.bt,namenode02.hdp03.bt");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set(TableInputFormat.INPUT_TABLE, "fb-profile-new");

        HBaseAdmin.checkHBaseAvailable(config);

        JavaRDD<Row> result = jsc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
                .map(data -> {
                    Result results = data._2();

                    String age = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("age"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("age"))) : "n/a";
                    String age_range = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("age_range"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("age_range"))) : "n/a";
                    String city = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("city"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("city"))) : "n/a";
                    String education = "n/a";
                    String friend = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("friend"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("friend"))) : "n/a";
                    String from_group = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("from"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("from"))) : "n/a";
                    String gender = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("gender"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("gender"))) : "n/a";
                    String groups = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("groups"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("groups"))) : "n/a";
                    String home_town = "n/a";
                    String hometown = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("hometown"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("hometown"))) : "n/a";
                    String id = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("id"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("id"))) : "n/a";
                    String joined = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("joined"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("joined"))) : "n/a";
                    String jokowi_precentage = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("jokowi_precentage"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("jokowi_precentage"))) : "n/a";
                    String latitude = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("latitude"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("latitude"))) : "n/a";
                    String longitude = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("longitude"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("longitude"))) : "n/a";
                    String name = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("name"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("name"))) : "n/a";
                    String prabowo_precentage = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("prabowo_precentage"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("prabowo_precentage"))) : "n/a";
                    String relationship_status = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("relationship_status"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("relationship_status"))) : "n/a";
                    String religion = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("religion"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("religion"))) : "n/a";
                    String school_name = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("school_name"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("school_name"))) : "n/a";
                    String state = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("state"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("state"))) : "n/a";
                    String tendency = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("tendency"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("tendency"))) : "n/a";
                    String tendency_total = (Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("tendency_total"))) != null) ? Bytes.toString(results.getValue(Bytes.toBytes("0"), Bytes.toBytes("tendency_total"))) : "n/a";
                    String work = "n/a";

                    return RowFactory.create(age, age_range, city, education, friend, from_group, gender, groups, home_town, hometown, id, joined, jokowi_precentage, latitude, longitude, name, prabowo_precentage, relationship_status
                    , religion, school_name, state, tendency, tendency_total, work);
                });

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("age", DataTypes.StringType, false),
                DataTypes.createStructField("age_range", DataTypes.StringType, false),
                DataTypes.createStructField("city", DataTypes.StringType, false),
                DataTypes.createStructField("education", DataTypes.StringType, false),
                DataTypes.createStructField("friend", DataTypes.StringType, false),
                DataTypes.createStructField("from_group", DataTypes.StringType, false),
                DataTypes.createStructField("gender", DataTypes.StringType, false),
                DataTypes.createStructField("groups", DataTypes.StringType, false),
                DataTypes.createStructField("home_town", DataTypes.StringType, false),
                DataTypes.createStructField("hometown", DataTypes.StringType, false),
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("joined", DataTypes.StringType, false),
                DataTypes.createStructField("jokowi_precentage", DataTypes.StringType, false),
                DataTypes.createStructField("latitude", DataTypes.StringType, false),
                DataTypes.createStructField("longitude", DataTypes.StringType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("prabowo_precentage", DataTypes.StringType, false),
                DataTypes.createStructField("relationship_status", DataTypes.StringType, false),
                DataTypes.createStructField("religion", DataTypes.StringType, false),
                DataTypes.createStructField("school_name", DataTypes.StringType, false),
                DataTypes.createStructField("state", DataTypes.StringType, false),
                DataTypes.createStructField("tendency", DataTypes.StringType, false),
                DataTypes.createStructField("tendency_total", DataTypes.StringType, false),
                DataTypes.createStructField("work", DataTypes.StringType, false)
        });

        Dataset<Row> df = sparkSession.createDataFrame(result, schema);

        Properties properties = new Properties();
        properties.put("user", "postgres");
        properties.put("password", "");

        df.write().mode(SaveMode.Overwrite).option(JDBCOptions.JDBC_DRIVER_CLASS(), "org.postgresql.Driver").jdbc("jdbc:postgresql://192.168.24.120:9700/postgres", "fb_profile_new", properties);


    }

}
