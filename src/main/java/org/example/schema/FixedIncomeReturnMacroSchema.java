package org.example.schema;

import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvBindByPosition;
import org.apache.pulsar.shade.com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.io.Serializable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class FixedIncomeReturnMacroSchema implements Serializable {
    public static final String ROUTING_KEY = "YEAR";

    @Override
    public String toString() {
        return "FixedIncomeReturnMacroSchema{" +
                "cid='" + cid + '\'' +
                ", xcat='" + xcat + '\'' +
                ", realDate='" + realDate + '\'' +
                ", value=" + value +
                '}';
    }

    public FixedIncomeReturnMacroSchema() {
    }

    @CsvBindByPosition(position = 1)
    private String cid;

    @CsvBindByPosition(position = 2)
    private String xcat;

    @CsvBindByPosition(position = 3)
    private String realDate;

    @CsvBindByPosition(position = 4)
    private float value;

    public String getCid() {
        return cid;
    }

    public String getXcat() {
        return xcat;
    }

    public String getRealDate() {
        return realDate;
    }

    public float getValue() {
        return value;
    }

    public String getYear() {
        // yyyy-mm-dd
        String[] parts = this.realDate.split("-");
        return parts[0];
    }
}



