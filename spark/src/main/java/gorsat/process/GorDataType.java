package gorsat.process;

import org.apache.spark.sql.types.DataType;

import java.util.List;
import java.util.Map;

public class GorDataType {
    public Map<Integer, DataType> dataTypeMap;
    public boolean withStart;
    public boolean nor;
    public String[] header;
    public String[] gortypes;
    public boolean base128;
    public List<String> usedFiles;

    public GorDataType(Map<Integer, DataType> dataTypeMap, boolean withStart, String[] header, String[] gortypes, boolean base128, boolean nor) {
        this.dataTypeMap = dataTypeMap;
        this.withStart = withStart;
        this.header = header;
        this.gortypes = gortypes;
        this.base128 = base128;
        this.nor = nor;
    }

    public GorDataType(Map<Integer, DataType> dataTypeMap, boolean withStart, String[] header, String[] gortypes, boolean nor) {
        this(dataTypeMap, withStart, header, gortypes, false, nor);
    }

    public void setUsedFiles(List<String> usedFiles) {
        this.usedFiles = usedFiles;
    }
}
