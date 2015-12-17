package geofusion.spark.samples;
import java.util.Arrays;

/* SimpleApp.java */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;

import geofusion.spark.samples.struct.Language;
import geofusion.spark.samples.struct.Person;

public class SqlSample {
	public static void main(String[] csvFiles) {
		SparkConf sparkConfig = new SparkConf()
				.setAppName("Teste do Spark")
				.setMaster("local[*]")
				//.setMaster("spark://master:7077")
				.set("spark.eventLog.enabled", "true")
				.setJars(new String[]{"/home/pcosta/sparkfun/spark-samples/target/test-spark-0.0.1-SNAPSHOT.jar"});
		
		try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig)) {
			SQLContext sqlSparkContext = new SQLContext(sparkContext);
			JavaRDD<Person> peopleRDD = sparkContext.parallelize(Arrays.asList(
					new Person("Kayleen Reetz"     , "C++"  , 35),
					new Person("Geraldo Gilmartin" , "JS"   , 22),
					new Person("Barbara Poehler"   , "Java" , 33),
					new Person("Allan Wohlford"    , "JS"   , 15),
					new Person("Alonzo Music"      , "C++"  , 40),
					new Person("Ashton Layden"     , "Java" , 30),
					new Person("Pete Murphy"       , "Cobol", 56)
			));
			
			JavaRDD<Language> languagesRDD = sparkContext.parallelize(Arrays.asList(
					new Language("Java", 24.4),
					new Language("C++" , 7.6),
					new Language("JS"  , 7.2),
					new Language("Cobol"  , 0.05)
			));
			
			
			
			DataFrame peopleDataframe = sqlSparkContext.createDataFrame(peopleRDD, Person.class);
			peopleDataframe.show();
			
			
			
			DataFrame languagesDataframe = sqlSparkContext.createDataFrame(languagesRDD, Language.class);
			languagesDataframe.show();
			
			
			
			DataFrame olderPeopleNamesAndYear = peopleDataframe
					.where(
							peopleDataframe.col("age").geq(30)
					)
					.orderBy(
							peopleDataframe.col("age")
					)
					.select( 
							peopleDataframe.col("name"), 
							peopleDataframe.col("age").multiply(-1).plus(2015).as("birth")
					);
			olderPeopleNamesAndYear.show();
			
			
			
			DataFrame olderPeopleNamesAndYear2 = peopleDataframe
					.where("age >= 30")
					.orderBy("age")
					.selectExpr("name", "age + 1 as birth");
			
			olderPeopleNamesAndYear2.show();
			
			
			
			DataFrame meanAgeByLanguage = peopleDataframe
					.groupBy("favoriteLanguage")
					.agg(functions.mean(peopleDataframe.col("age")).as("meanAge"))
					.orderBy("meanAge");
			meanAgeByLanguage.show();
			
			
			
			DataFrame peopleAndLanguagePopularity = peopleDataframe
					.join(
							languagesDataframe, 
							peopleDataframe.col("favoriteLanguage").equalTo(languagesDataframe.col("name")));
			peopleAndLanguagePopularity.show();
			
			
			
			peopleDataframe.registerTempTable("people"); //Associa um nome
			DataFrame olderPeopleNamesAndYear3 = sqlSparkContext
					.sql("select name, 2015-age as birth from people where age >= 30");
			olderPeopleNamesAndYear3.show();
		}
	}
}
