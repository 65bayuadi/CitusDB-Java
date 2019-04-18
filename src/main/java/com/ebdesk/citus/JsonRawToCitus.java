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

public class JsonRawToCitus {
    public static void main(String[] args) {

//        select();
        String path = args[0];
        String table = args[1];
//        String path = "D:/IdeaProjects/citus/assets/part-00001";
        SparkSession sparkSession = new SparkSession.Builder().getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<Row> rowRDD = sc.textFile(path).map(data -> {
            JSONObject jsonObject = new JSONObject(data);
            String key = jsonObject.get("rowKey").toString();
            String value = jsonObject.get("rawData").toString();

            return RowFactory.create(key, value);
        });

//        System.out.println(rowRDD.count());

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("key", DataTypes.StringType, false),
                DataTypes.createStructField("value", DataTypes.StringType, false)
        });

        Dataset<Row> df = sparkSession.createDataFrame(rowRDD, schema);

//        df.show();

//        System.out.println(df.count());

        Properties properties = new Properties();
        properties.put("user", "postgres");
        properties.put("password", "");

        df.write().mode(SaveMode.Overwrite).option(JDBCOptions.JDBC_DRIVER_CLASS(), "org.postgresql.Driver").jdbc("jdbc:postgresql://192.168.99.95:5432/postgres",table, properties);
    }
}
