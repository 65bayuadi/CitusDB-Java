package com.ebdesk.citus;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONObject;

import java.util.Properties;

public class JsonToCitus {
    public static void main(String[] args) {
        String path = args[0];
        String table = args[1];
        String stringManip = args[2];

        SparkSession sparkSession = new SparkSession.Builder().getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<Row> rowRDD = sc.textFile(path).map(data -> {
            JSONObject jsonObject = new JSONObject(data);
            String id = jsonObject.get("id").toString() + stringManip;
            String type = jsonObject.get("type").toString();
            String name = jsonObject.get("name").toString();
            String gender = jsonObject.get("gender").toString();
            String query_date = new java.text.SimpleDateFormat("yyyy-MM-dd").format(new java.util.Date(System.currentTimeMillis()));
            String friends_count = jsonObject.get("friends_count").toString();
            String school_name = jsonObject.get("school_name").toString();
            String location_hometown = jsonObject.get("location_hometown").toString();
            String location_city = jsonObject.get("location_city").toString();
            String location_province = jsonObject.get("location_province").toString();

            return RowFactory.create(id, type, name, gender, query_date, friends_count, school_name, location_hometown, location_city, location_province);
        });

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("type", DataTypes.StringType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("gender", DataTypes.StringType, false),
                DataTypes.createStructField("query_date", DataTypes.StringType, false),
                DataTypes.createStructField("friends_count", DataTypes.StringType, false),
                DataTypes.createStructField("school_name", DataTypes.StringType, false),
                DataTypes.createStructField("location_hometown", DataTypes.StringType, false),
                DataTypes.createStructField("location_city", DataTypes.StringType, false),
                DataTypes.createStructField("location_province", DataTypes.StringType, false)
        });
//
        Dataset<Row> df = sparkSession.createDataFrame(rowRDD, schema);

//        df.show();
//        df.printSchema();
//        System.out.println(df.count());

//        System.out.println(df.count());

        Properties properties = new Properties();
        properties.put("user", "postgres");
        properties.put("password", "");

        df.write().mode(SaveMode.Append).option(JDBCOptions.JDBC_DRIVER_CLASS(), "org.postgresql.Driver").jdbc("jdbc:postgresql://192.168.99.95:5432/postgres", table, properties);
    }
}
