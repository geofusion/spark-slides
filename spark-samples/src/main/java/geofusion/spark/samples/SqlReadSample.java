package geofusion.spark.samples;
/* SimpleApp.java */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class SqlReadSample {
	public static void main(String[] csvFiles) {
		SparkConf sparkConfig = new SparkConf()
				.setAppName("Teste do Spark")
				.setMaster("local[*]")
				//.setMaster("spark://master:7077")
				.set("spark.eventLog.enabled", "true")
				.setJars(new String[]{"/home/pcosta/sparkfun/spark-samples/target/test-spark-0.0.1-SNAPSHOT.jar"});
		
		try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig)) {
			SQLContext sqlSparkContext = new SQLContext(sparkContext);
			
			/*DataFrame jdbcDataframe = sqlSparkContext.read()
					.format("jdbc")
					.option("url", "jdbc:oracle:thin:user/password@phoenix:1521/gfn")
					.option("dbtable", "ADMIN.PRODUTO")
					.load();
			jdbcDataframe.show();			
			*/
			
			
			DataFrame csvDataframe = sqlSparkContext.read()
					.format("com.databricks.spark.csv")
					.option("header", "true")
					.load("/home/pcosta/Downloads/Parking_Citations.csv");
			csvDataframe.show();

	
			
			DataFrame mongoDataframe = sqlSparkContext.read()
					.format("com.stratio.datasource.mongodb")
					.option("host", "localhost")
					.option("database", "company_0")
					.option("collection", "smartReport")
					.load();
			mongoDataframe.show();

			
			
			csvDataframe.write()
					.format("com.stratio.datasource.mongodb")
					.option("host", "localhost")
					.option("database", "spark")
					.option("collection", "csv2mongo")
					.mode(SaveMode.Overwrite)
					.save();
		}
	}
}
