package org.gorpipe.spark;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class UTestSparkOperatorSpecs {

    @Test
    public void testSpecs() {
        SparkOperatorSpecs specs = new SparkOperatorSpecs();
        specs.addConfig("spec.test", 10);

        Map<String,Object> spec = new HashMap<>();
        spec.put("test",5);
        Map<String,Object> body = new HashMap<>();
        body.put("spec",spec);
        specs.apply(body);

        specs.apply(body);

        Assert.assertEquals("Wrong map merge", "{spec={test=5}}", body.toString());
    }
}
