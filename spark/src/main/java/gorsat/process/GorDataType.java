package gorsat.process;

import org.apache.spark.sql.types.DataType;

import java.util.List;
import java.util.Map;

public class GorDataType {
    public Map<Integer, DataType> dataTypeMap;
    public boolean withStart;
    public String[] header;
    public String[] gortypes;
    public boolean base128;
    public List<String> usedFiles;

    public GorDataType(Map<Integer, DataType> dataTypeMap, boolean withStart, String[] header, String[] gortypes, boolean base128) {
        this.dataTypeMap = dataTypeMap;
        this.withStart = withStart;
        this.header = header;
        this.gortypes = gortypes;
        this.base128 = base128;
    }

    public GorDataType(Map<Integer, DataType> dataTypeMap, boolean withStart, String[] header, String[] gortypes) {
        this(dataTypeMap, withStart, header, gortypes, false);
    }

    public void setUsedFiles(List<String> usedFiles) {
        this.usedFiles = usedFiles;
    }
}
