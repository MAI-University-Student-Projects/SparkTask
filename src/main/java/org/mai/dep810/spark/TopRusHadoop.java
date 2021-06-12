package org.mai.dep810.spark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import scala.reflect.ClassManifestFactory;

import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;

public class TopRusHadoop {
    public static void main(String[] args) {
        String postsPath = args[0];
        String usersPath = args[1];
        String outputPath = args[2];

        SparkSession spark = SparkSession
                .builder()
                .appName("TopRusHadoop")
                .getOrCreate();

        Dataset<Row> df_posts = spark.read()
                .format("xml")
                .option("rowTag", "row")
                .load(postsPath);

        Dataset<Row> df_users = spark.read()
                .format("xml")
                .option("rowTag", "row")
                .load(usersPath);

        List<String> location_from = Arrays.asList(
                "russia", "russian federation", "moscow", "saint petersburg",
                "россия", "москва", "санкт-петербург"
        );

        Dataset<Long> questid_hadoop_df = df_posts
                .filter(df_posts.col("_Tags").contains("brewing"))
                .map(row -> row.getAs("_Id"), Encoders.LONG());

        Broadcast<List<String>> broadcast_locations = spark.sparkContext().broadcast(location_from, ClassManifestFactory.classType(List.class));
        Broadcast<Dataset<Long>> broadcast_quest_ids = spark.sparkContext().broadcast(questid_hadoop_df, ClassManifestFactory.classType(Dataset.class));
        Dataset<Row> rus_users = df_users.filter((FilterFunction<Row>) row -> {
            Object tmp = row.getAs("_Location");
            if(tmp == null)
                return false;
            for(String loc : broadcast_locations.value()) {
                if(tmp.toString().toLowerCase().contains(loc))
                    return true;
            }
            return false;
        }).persist(StorageLevel.MEMORY_AND_DISK());

        df_posts
                .join(broadcast_quest_ids.value(), df_posts.col("_ParentId").equalTo(broadcast_quest_ids.value().col("value")), "inner")
                .join(rus_users, df_posts.col("_OwnerUserId").equalTo(rus_users.col("_Id")), "inner")
                .groupBy(rus_users.col("_DisplayName"), rus_users.col("_AccountId"), rus_users.col("_Location"))
                .sum("_Score")
                .sort(col("sum(_Score)").desc())
                .write()
                .format("xml")
                .option("rootTag","users")
                .option("rowTag","row")
                .save(outputPath);
    }
}
