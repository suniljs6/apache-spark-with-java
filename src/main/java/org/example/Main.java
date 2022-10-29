package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class Main {
    public static void main(String[] args) {
        var sparkSession = SparkSession.builder()
                                    .appName("Apache Spark example")
                                    .master("local[*]")
                                    .getOrCreate();

        Dataset<Row> json = sparkSession.read().option("multiline", true).json("src/main/java/org/example/sampleData.json");
        System.out.println(json.schema().prettyJson());

        Dataset<Row> filter = json.filter(col("firstName").equalTo("Alice").and(col("age").gt(24)));

        filter.show();

    }
}