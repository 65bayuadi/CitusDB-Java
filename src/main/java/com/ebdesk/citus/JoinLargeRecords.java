package com.ebdesk.citus;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;
import org.apache.log4j.Logger;

public class JoinLargeRecords {

    private static Logger logger = Logger.getRootLogger();

    public static void main(String[] args) throws ClassNotFoundException {
        SparkSession sparkSession = new SparkSession.Builder().appName("cek").master("local[2]").getOrCreate();
//        SparkSession sparkSession = new SparkSession.Builder().getOrCreate();

        logger.info("================== Running Engine ==================");

        Properties properties = new Properties();
        properties.put("driver", "org.postgresql.Driver");
        properties.put("user", "postgres");
        properties.put("password", "");

        Class.forName("org.postgresql.Driver");

        String table = "(SELECT * FROM fb LIMIT 10) as t";
        Dataset<Row> data = sparkSession.read().jdbc("jdbc:postgresql://192.168.99.95:5432/postgres", table, properties);
        System.out.println(data.count());

    }

    public static void dataCount(String table) throws ClassNotFoundException {
        SparkSession sparkSession = new SparkSession.Builder().getOrCreate();

        logger.info("================== Running Engine ==================");

        Properties properties = new Properties();
        properties.put("driver", "org.postgresql.Driver");
        properties.put("user", "postgres");
        properties.put("password", "");

        Class.forName("org.postgresql.Driver");

        Dataset<Row> jdbcDF2 = sparkSession.read().jdbc("jdbc:postgresql://192.168.99.95:5432/postgres", table, properties);

        int count = (int) jdbcDF2.count();

        logger.info("================== " + count + " ==================");
        System.out.println(count);
    }
}
