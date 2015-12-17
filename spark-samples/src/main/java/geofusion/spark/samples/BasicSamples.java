package geofusion.spark.samples;
import java.util.Arrays;
import java.util.List;

/* SimpleApp.java */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class BasicSamples {
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
			
			
			
			long count = numbers
				.count();
			System.out.println("count: " + count);
			
			
			
			List<Integer> numbersList = numbers
				.collect();
			System.out.println("Lista: " + numbersList);
				
				
			
			List<Integer> numberSample = numbers
				.takeSample(false, 5);
			System.out.println("Amostra: " + numberSample);
			
			
			
			int sum = numbers
				.reduce((a, b) -> a+b);
			System.out.println("Σx: " + sum);
			
			
			
			int sumSqr = numbers
					.map(x -> x*x).setName("x^2")
					.reduce((a, b) -> a+b);
			System.out.println("Σx²: " + sumSqr);
			
			
			
			JavaRDD<Integer> odd = numbers
					.filter(x -> x%2==1).setName("impares");
			long countOdd = odd.count();
			System.out.println("ímpares: " + countOdd + " elementos");
			
			
			
			List<Integer> valuesOdd = odd.collect();
			System.out.println("ímpares: " + valuesOdd);
			
			
			
			int sumOdd = odd
				.reduce((a, b) -> a+b);
			System.out.println("Σx | x ímpar: " + sumOdd);
		}
	}
}
