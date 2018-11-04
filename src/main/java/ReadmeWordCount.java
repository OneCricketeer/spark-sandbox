import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ReadmeWordCount extends BaseSparkDriver {

    public static void main(String[] args) {
        String sparkHome = getSparkHome(SPARK_VERSION);
        SparkSession spark = getSparkSession(ReadmeWordCount.class.getName(), false);

        Set<String> stopWords = new HashSet<>(Arrays.asList(
                "", "the", "and", "if", "with", "run", "of", "to", "for",
                "a", "an", "on", "is", "can", "you", "in", "also",
                "will", "not", "by", "must", "because", "its", "only", "have",
                "from", "your", "them", "than", "need", "are", "no", "do", "be",
                "at", "it", "this", "or", "use", "see", "general", "locally", "please", "how"));

        Dataset<String> filtered = spark.read().textFile(sparkHome + "/README.md")
                .flatMap(line -> Arrays.asList(line.split("\\W+")).iterator(), Encoders.STRING())
                .map(word -> word.toLowerCase().trim(), Encoders.STRING())
                .filter(s -> !stopWords.contains(s));

        Dataset<Row> df = filtered
                .map(word -> new Tuple2<>(word, 1L), Encoders.tuple(Encoders.STRING(), Encoders.LONG()))
                .toDF("word", "count")
                .groupBy("word")
                .sum("count").orderBy(new Column("sum(count)").desc()).withColumnRenamed("sum(count)", "_cnt");

        df.show(35, false);
    }

}
