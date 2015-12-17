package geofusion.spark.samples.csvimport;
import java.util.Scanner;

/* SimpleApp.java */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class CsvImportSample {
	public static void main(String[] csvFiles) {
		SparkConf sparkConfig = new SparkConf()
				.setAppName("Teste do Spark")
				.setMaster("local[*]")
				//.setMaster("spark://master:7077")
				.set("spark.eventLog.enabled", "true")
				.setJars(new String[]{"/home/pcosta/sparkfun/spark-samples/target/test-spark-0.0.1-SNAPSHOT.jar"});
		
		try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig)) {
			SQLContext sqlSparkContext = new SQLContext(sparkContext);
			
			for (String filename : csvFiles) {
				
				//Lê o arquivo CSV -- Todas as colunas são do tipo String
				DataFrame csvDataframe = sqlSparkContext.read()
						.format("com.databricks.spark.csv")
						.option("header", "true")
						.option("schema_samplingRatio", "0")
						.load(filename);
				
				//Inferência de tipos
				StructTypeInference types = csvDataframe
						.javaRDD()
						.aggregate(
								new StructTypeInference(csvDataframe.schema().size()),
								StructTypeInference::digest,
								StructTypeInference::merge);
				
				//Converte as colunas para os tipos corretos
				DataFrame convertedData = sqlSparkContext.createDataFrame(
						csvDataframe.javaRDD().map(types::convertRow),
						types.convertSchema(csvDataframe.schema()));

				//Salva os dados convertidos no mongo
				convertedData.write()
					.format("com.stratio.datasource.mongodb")
					.option("host", "localhost")
					.option("database", "spark")
					.option("collection", filename)
					.mode(SaveMode.Overwrite)
					.save();
				

				//Exibe amostra dos dados
				convertedData.show(100, false);
				System.out.println(types);

				new Scanner(System.in).nextLine();
			}
		}
	}
}
