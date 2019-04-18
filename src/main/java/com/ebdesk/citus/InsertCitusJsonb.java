package com.ebdesk.citus;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InsertCitusJsonb {

    private static List<String> listId = new ArrayList<>();

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        SparkSession sparkSession = new SparkSession.Builder().appName("cek").master("local[2]").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> rdd = jsc.textFile("D:\\IdeaProjects\\citus\\assets\\coba").map(data -> {
            return data.toString();
        });

        Object[] listData = rdd.collect().toArray();

        Connection c = null;
        Statement stmt = null;
        Class.forName("org.postgresql.Driver");
        c = DriverManager.getConnection("jdbc:postgresql://192.168.24.120:9700/postgres", "postgres", "");
        String sqlQuery = "INSERT INTO test AS t (key, value) VALUES (?, ?::jsonb) ON CONFLICT (key) DO UPDATE SET value = t.value || EXCLUDED.value";

        PreparedStatement pstmt = c.prepareStatement(sqlQuery);
        c.setAutoCommit(false);

        for (int i = 0; i < listData.length; i++) {
            if (i < 10) {
                JSONObject object = new JSONObject(listData[i].toString());
                pstmt.setString(1, object.get("id").toString());
                pstmt.setString(2, "[{\"name\" : \"" + object.get("name").toString() + "\"}]");
                pstmt.addBatch();
            } else {
                break;
            }
        }
        int[] tesult = new int[0];
        try {
            tesult = pstmt.executeBatch();
        } catch (SQLException e) {
            System.out.println(e.getNextException());
            e.printStackTrace();
        }
        c.commit();
        if(pstmt!=null)
            pstmt.close();
        if(c!=null)
            c.close();

        System.out.println("The number of rows inserted: "+ tesult.length);

//        Connection c = null;
//        Statement stmt = null;
//        System.out.println("beggin connection");
//        try {
//            Class.forName("org.postgresql.Driver");
//            c = DriverManager
//                    .getConnection("jdbc:postgresql://192.168.24.120:9700/postgres",
//                            "postgres", "");
//            c.setAutoCommit(false);
//            System.out.println("Opened database successfully");
//
//            stmt = c.createStatement();
//            ResultSet rs = stmt.executeQuery("SELECT * FROM test LIMIT 1;");
//
//            while (rs.next()) {
//                String id = rs.getString("key");
//                String name = rs.getString("value");
//
//                System.out.println("key = " + id);
//                System.out.println("value = " + name);
//                System.out.println();
//            }
//            rs.close();
//            stmt.close();
//            c.close();
//        } catch (Exception e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//        System.out.println("Operation done successfully");

//        rdd.foreach(result -> {
//            JSONObject cek = new JSONObject(result.toString());
//            System.out.println(cek.get("id"));
//        });
//
//        System.out.println(rdd.count());
    }
}
