package geofusion.spark.samples.struct;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Person implements Serializable {
	String name;
	String favoriteLanguage;
	int age;
	
	public Person(String name, String favoriteLanguage, int age) {
		this.name = name;
		this.favoriteLanguage = favoriteLanguage;
		this.age = age;
	}
	@Override
	public String toString() {
		return name + " (" + favoriteLanguage + ", " + age + ")";
	}
	
	public String getName() {
		return name;
	}
	public String getFavoriteLanguage() {
		return favoriteLanguage;
	}
	public int getAge() {
		return age;
	}
}