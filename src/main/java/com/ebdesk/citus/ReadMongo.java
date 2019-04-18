package com.ebdesk.citus;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;


public class ReadMongo {
    public static void main(String[] args) {
        SparkSession sparkSession = new SparkSession.Builder().appName("cek").master("local[2]")
                .config("spark.mongodb.input.uri", "mongodb://mongoisa:Rahas!aisa20!9@192.168.180.95:27027/bin-ecom-test.lazada?authSource=admin")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        rdd.foreach(data -> {
            System.out.println(data.toJson().toString());
        });
//        System.out.println(rdd.count());
//        System.out.println(rdd.first().toJson());

        jsc.close();

    }
}
