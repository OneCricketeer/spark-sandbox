import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Map;

public abstract class BaseSparkDriver {

    public static final String SPARK_VERSION = "2.3.2";
    public static final String SPARK_MASTER = "local[*]";

    protected static String getSparkHome(String sparkVersion) {
        return "/usr/local/Cellar/apache-spark/" + sparkVersion;
    }

    protected static SparkConf getSparkConf(String master, List<Map.Entry<String, String>> sparkConfigs) {
        final String sparkHome = getSparkHome(SPARK_VERSION);
        SparkConf conf = new SparkConf()
                .setSparkHome(sparkHome + "/libexec")
                .setMaster(master);

        if (sparkConfigs != null) {
            for (Map.Entry<String, String> config : sparkConfigs) {
                conf.set(config.getKey(), config.getValue());
            }
        }

        return conf;
    }

    protected static SparkSession getSparkSession(String appName, boolean enableHive) {
        SparkConf conf = getSparkConf(SPARK_MASTER, null).setAppName(appName);
        SparkSession.Builder spark = SparkSession.builder().config(conf);
        if (enableHive) {
            spark = spark.enableHiveSupport();
        }
        return spark.getOrCreate();
    }
}
