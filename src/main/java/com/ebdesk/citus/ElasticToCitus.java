package com.ebdesk.citus;

import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import scala.Tuple2;

import java.sql.*;
import java.util.Properties;

public class ElasticToCitus {

//    Data not inserted without error message

    public static void main(String[] args) {
        SparkSession sparkSession = new SparkSession.Builder().appName("cek").master("local[2]").config("es.nodes", "192.168.24.31").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

//        JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(sc, "facebook-profile/_doc");

//        JavaRDD<Row> esRDD = JavaEsSpark.esRDD(sc, "facebook-profile/_doc", "?q=*").values()
//                .map(data -> {
//                    JSONObject temp = new JSONObject(data);
//                    JSONObject fixed = new JSONObject();
//                    JSONArray education = new JSONArray();
//
//                    fixed.put("id", ((temp.has("id")) ? temp.get("id").toString() : "n/a"));
//                    fixed.put("type", ((temp.has("type")) ? temp.get("type").toString() : "n/a"));
//                    fixed.put("name", ((temp.has("name")) ? temp.get("name").toString() : "n/a"));
//                    fixed.put("gender", ((temp.has("gender")) ? temp.get("gender").toString() : "n/a"));
//                    fixed.put("query_date", new java.text.SimpleDateFormat("yyyy-MM-dd").format(new java.util.Date(System.currentTimeMillis())).toString());
//                    fixed.put("friends_count", ((temp.getJSONObject("friends").has("count")) ? temp.getJSONObject("friends").get("count").toString().toString() : "n/a"));
//                    if (temp.has("education")) {
//                        JSONArray eduTemp = new JSONArray();
//                        eduTemp = temp.getJSONArray("education");
//                        for (int i = 0; i < eduTemp.length(); i++) {
//                            JSONObject eduObj = new JSONObject(eduTemp.get(i).toString());
//                            if (eduObj.has("school")) {
//                                try {
//                                    education.put(eduObj.getJSONObject("school").get("name"));
//                                } catch (JSONException e) {
////                                    e.printStackTrace();
//                                }
//
//                            }
//                        }
//                    }
//                    fixed.put("school_name", "n/a");
//                    if (temp.has("location")) {
//                        fixed.put("location_hometown", ((temp.getJSONObject("location").has("hometown")) ? temp.getJSONObject("location").get("hometown").toString() : "n/a"));
//                        fixed.put("location_city", ((temp.getJSONObject("location").has("city")) ? temp.getJSONObject("location").get("city").toString() : "n/a"));
//                        fixed.put("location_province", ((temp.getJSONObject("location").has("province")) ? temp.getJSONObject("location").get("province").toString() : "n/a"));
//                    } else {
//                        fixed.put("location_hometown", "n/a");
//                        fixed.put("location_city", "n/a");
//                        fixed.put("location_province", "n/a");
//                    }
//
//                    return RowFactory.create(fixed.get("id"), fixed.get("type"), fixed.get("name"), fixed.get("gender"), fixed.get("query_date"),
//                            fixed.get("friends_count"), fixed.get("school_name"), fixed.get("location_hometown"), fixed.get("location_city"), fixed.get("location_province"));
//                });
//
//        StructType schema = DataTypes.createStructType(new StructField[]{
//                DataTypes.createStructField("id", DataTypes.StringType, false),
//                DataTypes.createStructField("type", DataTypes.StringType, false),
//                DataTypes.createStructField("name", DataTypes.StringType, false),
//                DataTypes.createStructField("gender", DataTypes.StringType, false),
//                DataTypes.createStructField("query_date", DataTypes.StringType, false),
//                DataTypes.createStructField("friends_count", DataTypes.StringType, false),
//                DataTypes.createStructField("school_name", DataTypes.StringType, false),
//                DataTypes.createStructField("location_hometown", DataTypes.StringType, false),
//                DataTypes.createStructField("location_city", DataTypes.StringType, false),
//                DataTypes.createStructField("location_province", DataTypes.StringType, false)
//        });
//
//        Dataset<Row> df = sparkSession.createDataFrame(esRDD, schema);

        JavaRDD<Row> rowRDD = JavaEsSpark.esRDD(sc, "facebook-profile/_doc", "?q=*").values().map(data -> {
            JSONObject temp = new JSONObject(data);
            String id = ((temp.has("id")) ? temp.get("id").toString() : "n/a");
            String name = ((temp.has("name")) ? temp.get("name").toString() : "n/a");

            return RowFactory.create(id, name);
        });

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false)
        });

        Dataset<Row> df = sparkSession.createDataFrame(rowRDD, schema);

//        df.show();

        Properties properties = new Properties();
        properties.put("user", "postgres");
        properties.put("password", "");

        df.write().mode(SaveMode.Overwrite).option(JDBCOptions.JDBC_DRIVER_CLASS(), "org.postgresql.Driver").jdbc("jdbc:postgresql://192.168.99.95:5432/postgres","cek", properties);
    }

}
