package geofusion.spark.samples.csvimport;

import java.sql.Timestamp;
import java.util.EnumSet;
import java.util.Set;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import com.cedarsoftware.util.SafeSimpleDateFormat;

public enum ColumnTypeInference {
	UNKNOWN(null) {
		@Override
		public Set<ColumnTypeInference> getCompatibleTypes() {
			return EnumSet.of(LONG,  DOUBLE, TIMESTAMP, TIMESTAMP_TZ, STRING);
		}
		protected Object parseImpl(String value) throws UnsupportedOperationException {
			return new UnsupportedOperationException("Cannot parse unknown data type");
		}
	},
	
	LONG(DataTypes.LongType) {
		@Override
		public Set<ColumnTypeInference> getCompatibleTypes() {
			return EnumSet.of(LONG, DOUBLE, STRING);
		}
		@Override
		protected Long parseImpl(String value) throws NumberFormatException {
			return Long.parseLong(value);
		}
	},
	
	DOUBLE(DataTypes.DoubleType) {
		@Override
		public Set<ColumnTypeInference> getCompatibleTypes() {
			return EnumSet.of(DOUBLE, STRING);
		}
		@Override
		protected Double parseImpl(String value) throws NumberFormatException {
			return Double.parseDouble(value);
		}
	},
	
	TIMESTAMP_TZ(DataTypes.TimestampType) {
		private SafeSimpleDateFormat dateFormat = new SafeSimpleDateFormat("MM/dd/yyyy hh:mm:ss aa Z");
		
		@Override
		public Set<ColumnTypeInference> getCompatibleTypes() {
			return EnumSet.of(TIMESTAMP_TZ, STRING);
		}
		@Override
		protected Timestamp parseImpl(String value) throws Exception {
			return new Timestamp(dateFormat.parse(value).getTime());
		}
	},
	
	TIMESTAMP(DataTypes.TimestampType) {
		private SafeSimpleDateFormat dateFormat = new SafeSimpleDateFormat("MM/dd/yyyy hh:mm:ss aa");
		
		@Override
		public Set<ColumnTypeInference> getCompatibleTypes() {
			return EnumSet.of(TIMESTAMP, STRING);
		}
		@Override
		protected Timestamp parseImpl(String value) throws Exception {
			return new Timestamp(dateFormat.parse(value).getTime());
		}
	}, 
	
	STRING(DataTypes.StringType) {
		@Override
		public Set<ColumnTypeInference> getCompatibleTypes() {
			return EnumSet.of(STRING);
		}
		
		@Override
		protected String parseImpl(String value) {
			return value;
		}
	};
	
	private DataType sparkSqlType;
	
	private ColumnTypeInference(DataType sparkSqlType) {
		this.sparkSqlType = sparkSqlType;
	}
	
	public DataType getSparkSqlType() {
		return sparkSqlType;
	}

	protected abstract Object parseImpl(String value) throws Exception;
	
	public abstract Set<ColumnTypeInference> getCompatibleTypes();	
	
	public Object parse(String value) throws Exception {
		if (value == null || value.isEmpty()) {
			return null;
		} else {
			return parseImpl(value);
		}
	}
	
	public ColumnTypeInference digest(String value) {
		for (ColumnTypeInference dt : getCompatibleTypes()) {
			try {
				dt.parse(value);
				return dt;
			} catch (Exception e) {
				if (this == TIMESTAMP) {
					System.err.println("'" + value + "' is not a " + dt);
				}
			}
		}
		return STRING;
	}
	
	public ColumnTypeInference merge(ColumnTypeInference other) {
		Set<ColumnTypeInference> myCompatibleTypes = this.getCompatibleTypes();
		Set<ColumnTypeInference> otherCompatibleTypes = other.getCompatibleTypes();
		for (ColumnTypeInference dt : myCompatibleTypes) {
			if (otherCompatibleTypes.contains(dt)) {
				return dt;
			}
		}
		return STRING;
	}
}
