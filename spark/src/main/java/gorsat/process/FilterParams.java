package gorsat.process;

import java.io.Serializable;

public class FilterParams implements Serializable {
    FilterParams(String paramString, String[] headersplit, String[] colType) {
        this.paramString = paramString;
        this.headersplit = headersplit;
        this.colType = colType;
    }

    public String paramString;
    String[] headersplit;
    String[] colType;
}
