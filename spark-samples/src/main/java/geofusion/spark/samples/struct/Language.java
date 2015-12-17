package geofusion.spark.samples.struct;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Language implements Serializable {
	String name;
	double score;

	public Language(String name, double score) {
		this.name = name;
		this.score = score;
	}
	@Override
	public String toString() {
		return name + " (" + score + ")";
	}
	
	public String getName() {
		return name;
	}
	public double getScore() {
		return score;
	}
}