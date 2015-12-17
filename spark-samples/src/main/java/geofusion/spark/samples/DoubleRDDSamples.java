package geofusion.spark.samples;
import java.util.Arrays;

/* SimpleApp.java */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;

import scala.Tuple2;

public class DoubleRDDSamples {
	public static void printHistogram(double[] ranges, long[] counts) {
		System.out.println("Histogram - Ranges=" + Arrays.toString(ranges) + ", Counts=" + Arrays.toString(counts));
		for (int i=0; i<ranges.length-1; i++) {
			System.out.println("[ " + ranges[i] + "..." + ranges[i+1] + (i==ranges.length-2? " ]" : " )") + " - " + counts[i]);
		}
	}
	
	public static void main(String[] csvFiles) {
		SparkConf sparkConfig = new SparkConf()
				.setAppName("Teste do Spark")
				.setMaster("local[*]")
				//.setMaster("spark://master:7077")
				.set("spark.eventLog.enabled", "true")
				.setJars(new String[]{"/home/pcosta/sparkfun/spark-samples/target/test-spark-0.0.1-SNAPSHOT.jar"});
		
		try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig)) {		
			JavaDoubleRDD doubleNumbers = sparkContext
					.parallelizeDoubles(Arrays.asList(0.0,1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0))
					.setName("Double Numbers");

			StatCounter stats = doubleNumbers.stats();
			System.out.println("Stats: " + stats);
			
			Tuple2<double[], long[]> histogram = doubleNumbers.histogram(4);
			printHistogram(histogram._1(), histogram._2());
		}
	}
}
