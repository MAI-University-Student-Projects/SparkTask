package org.mai.dep810.spark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.reflect.ClassManifestFactory;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class LocalRunDebug {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("TopRusHadoop")
                .getOrCreate();

        Dataset<Row> beer_df = spark.read()
                .format("xml")
                .option("rowTag", "row")
                .load("local_data/PostsBeer_debug.xml");

        Dataset<Row> user_df = spark.read()
                .format("xml")
                .option("rowTag", "row")
                .load("local_data/UsersBeer_debug.xml");

        List<String> location_from = Arrays.asList("london", "new york", "usa", "united states");

        Dataset<Long> idx_brewing_df = beer_df
                .filter(beer_df.col("_Tags").contains("brewing"))
                .map(row -> row.getAs("_Id"), Encoders.LONG());

        Broadcast<List<String>> broadcast_locations = spark.sparkContext().broadcast(location_from, ClassManifestFactory.classType(List.class));
        Broadcast<Dataset<Long>> broadcast_brewing_id = spark.sparkContext().broadcast(idx_brewing_df, ClassManifestFactory.classType(Dataset.class));

        Dataset<Row> user_loc = user_df.filter((FilterFunction<Row>) row -> {
            Object tmp = row.getAs("_Location");
            if(tmp == null)
                return false;
            for(String loc : broadcast_locations.value()) {
                if(tmp.toString().toLowerCase().contains(loc))
                    return true;
            }
            return false;
        }).persist(StorageLevel.MEMORY_ONLY());

        beer_df
//                .filter(beer_df.col("_ParentId").isin(broadcast_brewing_id.value())
//                        .and(beer_df.col("_OwnerUserId").isNotNull()))
                .join(broadcast_brewing_id.value(), beer_df.col("_ParentId").equalTo(broadcast_brewing_id.value().col("value")), "inner")
                .join(user_loc, beer_df.col("_OwnerUserId").equalTo(user_loc.col("_Id")))
                .groupBy(user_loc.col("_DisplayName"), user_loc.col("_AccountId"), user_loc.col("_Location"))
                .sum("_Score")
                .sort(col("sum(_Score)").desc())
                .write()
                .format("xml")
                .option("rootTag","users")
                .option("rowTag","row")
                .save("result");
    }
}


