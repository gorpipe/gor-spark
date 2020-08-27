package org.gorpipe.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import io.kubernetes.client.util.Config;
import org.gorpipe.gor.driver.DataSource;
import org.gorpipe.gor.model.DriverBackedFileReader;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SparkOperatorRunner {
    CustomObjectsApi apiInstance;
    ObjectMapper objectMapper;
    String jobName;

    public SparkOperatorRunner() throws IOException {
        ApiClient client = Config.defaultClient();
        Configuration.setDefaultApiClient(client);
        apiInstance = new CustomObjectsApi();
        objectMapper = new ObjectMapper();
    }

    Map<String, Object> loadBody(String query, String project, String result_dir, Map<String, Object> parameters) throws IOException {
        Map<String, Object> body;
        body = objectMapper.readValue(query, Map.class);
        Map<String, Object> metadata = (Map<String, Object>) body.get("metadata");
        jobName = metadata.get("name").toString();
        if (body.containsKey("yaml")) {
            objectMapper = new ObjectMapper(new YAMLFactory());
            String yamlPathString = body.get("yaml").toString();
            Path queryRoot = Paths.get(project);
            Path yamlPath = Paths.get(yamlPathString);
            yamlPath = queryRoot.resolve(yamlPath);
            DriverBackedFileReader driverBackedGorServerFileReader = new DriverBackedFileReader("", project, null);
            DataSource yamlDataSource = driverBackedGorServerFileReader.resolveUrl(yamlPath.toString());
            String yamlContent = driverBackedGorServerFileReader.readFile(yamlDataSource.getSourceReference().getUrl()).collect(Collectors.joining("\n"));

            Map<String, Object> yaml = objectMapper.readValue(yamlContent, Map.class);
            Map<String, Object> md = (Map) yaml.get("metadata");
            String name = (String) md.get("name");
            if (name.equals("${name.val}")) md.put("name", jobName);

            Map<String, Object> specMap = (Map) body.get("spec");
            List<String> arguments = (List) specMap.get("arguments");

            Map<String, Object> spec = (Map) yaml.get("spec");
            Object args = spec.get("arguments");
            if (args.toString().equals("${arguments.val}")) {
                spec.put("arguments", arguments);
            }
            if (name.equals("${name.val}")) md.put("name", jobName);

            /*yamlContent = yamlContent.replace("${name.val}", jobName);

            String argString = objectMapper.writeValueAsString(arguments);
            yamlContent = yamlContent.replace("${arguments.val}", argString);*/

            yamlContent = objectMapper.writeValueAsString(yaml).substring(4);
            for (Map.Entry entry : parameters.entrySet()) {
                yamlContent = yamlContent.replace("${" + entry.getKey() + ".val}", entry.getValue().toString());
            }

            body = objectMapper.readValue(yamlContent, Map.class);
        }

        if (body.containsKey("spec")) {
            Map<String, Object> specMap = (Map) body.get("spec");
            if (specMap.containsKey("arguments")) {
                List<String> arguments = (List) specMap.get("arguments");
                int i = arguments.indexOf("#{result_dir}");
                if (i != -1) arguments.set(i, result_dir);
            }
            if (specMap.containsKey("executor")) {
                Map<String, Object> executor = (Map) specMap.get("executor");
                Object cores = executor.get("cores");
                if (cores instanceof String) executor.put("cores", Integer.parseInt(cores.toString()));
                Object instances = executor.get("instances");
                if (instances instanceof String) executor.put("instances", Integer.parseInt(instances.toString()));
            }
        }

        return body;
    }

    public void run(String yaml, String projectroot) throws IOException, ApiException {
        if (projectroot == null || projectroot.length() == 0)
            projectroot = Paths.get(".").toAbsolutePath().normalize().toString();
        Map<String, Object> body = loadBody(yaml, projectroot, "", new HashMap<>());
        apiInstance.createNamespacedCustomObject("sparkoperator.k8s.io", "v1beta2", "spark", "sparkapplications", body, "true", null, null);
    }
}
