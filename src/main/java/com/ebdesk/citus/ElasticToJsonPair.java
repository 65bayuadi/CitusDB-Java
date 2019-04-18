package com.ebdesk.citus;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;

public class ElasticToJsonPair {
    public static void main(String[] args) {
        SparkSession sparkSession = new SparkSession.Builder().appName("cek").master("local[2]").config("es.nodes", "192.168.24.31").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(sc, "facebook-profile/_doc");

        esRDD.coalesce(10).saveAsTextFile("D:\\IdeaProjects\\citus\\assets\\dumped");
    }



}
