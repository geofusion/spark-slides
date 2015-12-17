package geofusion.spark.samples.csvimport;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

@SuppressWarnings("serial")
public class StructTypeInference implements Serializable {
	ColumnTypeInference[] types;
	public StructTypeInference(int columns) {
		types = new ColumnTypeInference[columns];
		for (int i=0; i<columns; i++) {
			types[i] = ColumnTypeInference.UNKNOWN;
		}
	}
	
	public String toString() {
		return Arrays.toString(types);
	};
	
	public List<ColumnTypeInference> getTypes() {
		return Arrays.asList(types);
	}
	
	public StructTypeInference digest(Row row) {
		for (int i=0; i<types.length; i++) {
			types[i] = types[i].digest(row.getString(i));
		}
		return this;
	}
	
	public StructTypeInference merge(StructTypeInference other) {
		for (int i=0; i<types.length; i++) {
			types[i] = types[i].merge(other.types[i]);
		}
		return this;
	}
	
	
	public Row convertRow(Row row) throws Exception {
		Object[] ret = new Object[types.length];
		for (int i=0; i<types.length; i++) {
			ret[i] = types[i].parse(row.getString(i));
		}
		return RowFactory.create(ret);
	}
	public StructType convertSchema(StructType original) {
		List<StructField> fields = new ArrayList<>();
		for (int i=0; i<types.length; i++) {
			fields.add(DataTypes.createStructField(
					original.fieldNames()[i], 
					types[i].getSparkSqlType(),
					true));
		}
		return DataTypes.createStructType(fields);
	}
}
