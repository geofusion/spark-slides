package geofusion.spark.samples;
import java.util.Arrays;

/* SimpleApp.java */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ZipSample {
	public static void main(String[] csvFiles) {
		SparkConf sparkConfig = new SparkConf()
				.setAppName("Teste do Spark")
				//.setMaster("local[*]")
				.setMaster("spark://master:7077")
				.set("spark.eventLog.enabled", "true")
				.setJars(new String[]{"/home/pcosta/sparkfun/spark-samples/target/test-spark-0.0.1-SNAPSHOT.jar"});
		
		try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig)) {		
			JavaRDD<Integer> numbers = sparkContext
					.parallelize(Arrays.asList(0,1,2,3,4,5,6,7,8,9,10))
					.setName("Numbers");

			JavaRDD<Double> sin2 = numbers 
					.map(Math::sin).setName("sin(x)")
					.map(x -> x*x).setName("sin^2(x)");
			JavaRDD<Double> cos2 = numbers
					.map(Math::cos).setName("cos(x)")
					.map(x -> x*x).setName("cos^2(x)");
			JavaPairRDD<Double, Double> cos2_sin2 = sin2
					.zip(cos2).setName("[ sin^2(x), cos^2(x) ]");
			JavaRDD<Double> ones = cos2_sin2
					.map(a -> a._1() + a._2())
					.setName("sin^2(x) + cos^2(x) = 1");
			double count2 = ones.reduce((a,b) -> a+b);
			System.out.println("Sin^2(x) + Cos^2(x): " + ones.collect());
			System.out.println("Count: " + count2);
		}
	}
}
