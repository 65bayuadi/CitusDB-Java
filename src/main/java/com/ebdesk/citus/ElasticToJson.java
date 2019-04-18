package com.ebdesk.citus;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ElasticToJson {
    public static void main(String[] args) {
//        String esNodes = args[0];
//        String esIndex = args[1];
//        String dumpPath = args[2];

        SparkSession sparkSession = new SparkSession.Builder().appName("cek").master("local[2]").config("es.nodes", "192.168.24.31").config("es.nodes.discovery", "false").config("es.nodes.wan.only", "true").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<JSONObject> esRDD = JavaEsSpark.esRDD(sc, "facebook-profile/_doc", "?q=*").values()
                .map(data -> {
                    JSONObject temp = new JSONObject(data);
                    JSONObject fixed = new JSONObject();
                    JSONArray education = new JSONArray();

                    System.out.println( temp.get("id").toString());

                    fixed.put("id", ((temp.has("id")) ? temp.get("id").toString() : "n/a"));
                    fixed.put("type", ((temp.has("type")) ? temp.get("type").toString() : "n/a"));
                    fixed.put("name", ((temp.has("name")) ? temp.get("name").toString() : "n/a"));
                    fixed.put("gender", ((temp.has("gender")) ? temp.get("gender").toString() : "n/a"));
                    fixed.put("query_date", new java.text.SimpleDateFormat("yyyy-MM-dd").format(new java.util.Date(System.currentTimeMillis())).toString());
                    fixed.put("friends_count", ((temp.getJSONObject("friends").has("count")) ? temp.getJSONObject("friends").get("count").toString().toString() : "n/a"));
                    if (temp.has("education")) {
                        JSONArray eduTemp = new JSONArray();
                        eduTemp = temp.getJSONArray("education");
                        for (int i = 0; i < eduTemp.length(); i++) {
                            JSONObject eduObj = new JSONObject(eduTemp.get(i).toString());
                            if (eduObj.has("school")) {
                                try {
                                    education.put(eduObj.getJSONObject("school").get("name"));
                                } catch (JSONException e) {
//                                    e.printStackTrace();
                                }

                            }
                        }
                    }
                    fixed.put("school_name", education);
                    if (temp.has("location")) {
                        try {
                            fixed.put("location_hometown", ((temp.getJSONObject("location").has("hometown")) ? temp.getJSONObject("location").get("hometown").toString() : "n/a"));
                            fixed.put("location_city", ((temp.getJSONObject("location").has("city")) ? temp.getJSONObject("loc22ation").get("city").toString() : "n/a"));
                            fixed.put("location_province", ((temp.getJSONObject("location").has("province")) ? temp.getJSONObject("location").get("province").toString() : "n/a"));
                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                    } else {
                        fixed.put("location_hometown", "n/a");
                        fixed.put("location_city", "n/a");
                        fixed.put("location_province", "n/a");
                    }

                    System.out.println("done");
                    return fixed;
                });

//        esRDD.coalesce(10).saveAsTextFile("D:\\IdeaProjects\\citus\\assets\\dumped");
    }
}
