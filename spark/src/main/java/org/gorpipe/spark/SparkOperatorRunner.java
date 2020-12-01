package org.gorpipe.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.gson.reflect.TypeToken;
import gorsat.Utilities.StringUtilities;
import gorsat.process.SparkPipeInstance;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import io.kubernetes.client.openapi.models.V1DeleteOptions;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Watch;
import okhttp3.Call;
import org.apache.spark.sql.SparkSession;
import org.gorpipe.exceptions.GorSystemException;
import org.gorpipe.gor.driver.DataSource;
import org.gorpipe.gor.model.DriverBackedFileReader;
import org.gorpipe.gor.monitor.GorMonitor;
import org.gorpipe.gor.util.Util;
import org.gorpipe.spark.redis.RedisBatchConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class SparkOperatorRunner {
    private static final Logger log = LoggerFactory.getLogger(SparkOperatorRunner.class);

    public static final String SPARKAPPLICATION_COMPLETED_STATE = "COMPLETED";
    public static final String SPARKAPPLICATION_FAILED_STATE = "FAILED";
    public static final String SPARKAPPLICATION_RUNNING_STATE = "RUNNING";

    ApiClient client;
    CustomObjectsApi apiInstance;
    CoreV1Api core;
    ObjectMapper objectMapper;
    String jobName;
    String namespace;
    boolean hostMount = false;
    SparkSession sparkSession;

    private static final boolean debug = false;

    public SparkOperatorRunner(GorSparkSession gorSparkSession) throws IOException {
        client = Config.defaultClient();
        Configuration.setDefaultApiClient(client);
        apiInstance = new CustomObjectsApi();
        core = new CoreV1Api(client);
        objectMapper = new ObjectMapper(new YAMLFactory());
        sparkSession = gorSparkSession.getSparkSession();
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
            namespace = md.containsKey("namespace") ? md.get("namespace").toString() : "gorkube";
            if (name.equals("${name.val}")) md.put("name", jobName);

            Map<String, Object> specMap = (Map) body.get("spec");
            List<String> arguments = (List) specMap.get("arguments");

            Map<String, Object> spec = (Map) yaml.get("spec");
            Object args = spec.get("arguments");
            if (args.toString().equals("${arguments.val}")) {
                spec.put("arguments", arguments);
            }
            if (name.equals("${name.val}")) md.put("name", jobName);

            yamlContent = objectMapper.writeValueAsString(yaml).substring(4);
            for (Map.Entry entry : parameters.entrySet()) {
                yamlContent = yamlContent.replace("${" + entry.getKey() + ".val}", entry.getValue().toString());
            }

            body = objectMapper.readValue(yamlContent, Map.class);
        } else {
            namespace = metadata.containsKey("namespace") ? metadata.get("namespace").toString() : "gorkube";
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

    public String getSparkApplicationState(String name) throws ApiException {
        try {
            Object obj = apiInstance.getNamespacedCustomObject("sparkoperator.k8s.io", "v1beta2", namespace, "sparkapplications", name);
            Map map = (Map)obj;
            Map statusMap = (Map)map.get("status");
            if(statusMap!=null) {
                Map appMap = (Map) statusMap.get("applicationState");
                return (String) appMap.get("state");
            }
        } catch (ApiException e) {
            if(!e.getMessage().contains("Not Found")) throw e;
        }
        return "";
    }

    public void deleteSparkApplication(String name) throws ApiException {
        V1DeleteOptions body = new V1DeleteOptions();
        apiInstance.deleteNamespacedCustomObject("sparkoperator.k8s.io", "v1beta2", namespace, "sparkapplications", name, null, null, null, null, body);
    }

    public boolean waitForSparkApplicationToComplete(GorMonitor mon, String name) throws ApiException, InterruptedException {
        String state = getSparkApplicationState(name);
        while (!state.equals(SPARKAPPLICATION_COMPLETED_STATE)) {
            if (state.equals(SPARKAPPLICATION_FAILED_STATE)) {
                throw new GorSystemException(state, null);
            }
            if (mon!=null && mon.isCancelled()) {
                deleteSparkApplication(name);
                return false;
            }
            Thread.sleep(1000);
            state = getSparkApplicationState(name);
        }
        return true;
    }

    void waitSparkApplicationState(GorMonitor mon,String name,String state) throws ApiException {
        Call call = core.listNamespacedPodCall(namespace, null, null, null, null, null, null, null, 120, null, null);
        try (Watch<V1Pod> watchSparkApplication = Watch.createWatch(client, call, new TypeToken<Watch.Response<V1Pod>>() {}.getType())) {
            for (Watch.Response<V1Pod> item : watchSparkApplication) {
                if (item.type != null && item.type.equals("MODIFIED") && item.object != null && item.object.getStatus() != null) {
                    String phase = item.object.getStatus().getPhase();
                    if (state.equals(phase)) {
                        break;
                    } else if ("Failed".equals(phase) || "Error".equals(phase)) {
                        throw new GorSystemException(item.object.toString(), null);
                    }
                }
                if (mon != null && mon.isCancelled()) {
                    deleteSparkApplication(name);
                }
            }
        } catch (Exception e) {
            // Ignore watch errors
            log.error(e.getMessage(), e);
        }
    }

    public void createSparkApplicationFromJson(String json) throws ApiException, JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        createSparkApplication(mapper, json);
    }

    public void createSparkApplicationFromYaml(String yaml) throws ApiException, JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        createSparkApplication(mapper, yaml);
    }

    public void createSparkApplication(ObjectMapper mapper, String contents) throws ApiException, JsonProcessingException {
        Object jsonObject = mapper.readValue(contents, Object.class);
        apiInstance.createNamespacedCustomObject("sparkoperator.k8s.io", "v1beta2", namespace, "sparkapplications", jsonObject, "true", null, null);
    }

    public static String getSparkOperatorYaml(String projectDir) throws IOException {
        String json = null;
        try {
            Path p = Paths.get(projectDir);
            if (Files.exists(p)) {
                Path so_json = p.resolve("config/sparkoperator.yaml");
                if (Files.exists(so_json)) json = new String(Files.readAllBytes(so_json));
            }
        } finally {
            if (json == null) {
                json = Util.readAndCloseStream(SparkPipeInstance.class.getResourceAsStream("sparkoperator.yaml"));
            }
        }
        return json;
    }

    public Path run(String uristr, String requestId, String projectDir, GorMonitor gm, String[] commands, String[] resourceSplit, String cachefile) throws IOException, ApiException, InterruptedException {
        String queries;
        if(commands.length>1) {
            queries = String.join(";", Arrays.copyOfRange(commands,0,commands.length-1)) + ";" + resourceSplit[0];
        } else {
            queries = resourceSplit[0];
        }
        String fingerprint = StringUtilities.createMD5(queries);

        Path cachefilepath;
        Path projectPath = Paths.get(projectDir);
        if(cachefile==null) {
            Path cachePath = projectPath.resolve("result_cache");
            String cachefiles = fingerprint + ".parquet";
            cachefilepath = cachePath.resolve(cachefiles);
            cachefile = cachefilepath.toAbsolutePath().normalize().toString();
        } else cachefilepath = Paths.get(cachefile);
        String jobid = fingerprint;

        if(!Files.exists(cachefilepath)) {
            SparkOperatorSpecs sparkOperatorSpecs = new SparkOperatorSpecs();

            List<Map<String, Object>> vollist = new ArrayList<>();
            vollist.add(Map.of("name", "volnfs", "hostPath", Map.of("path", projectDir, "type", "Directory")));
            sparkOperatorSpecs.addConfig("spec.volumes", vollist);

            List<Map<String, Object>> listMounts = new ArrayList<>();
            listMounts.add(Map.of("name", "volnfs", "mountPath", projectDir));
            sparkOperatorSpecs.addConfig("spec.executor.volumeMounts", listMounts);
            sparkOperatorSpecs.addConfig("spec.driver.volumeMounts", listMounts);

            Path projectBasePath = Paths.get("/mnt/csa");
            Path projectRealPath = projectPath.toRealPath().toAbsolutePath();
            Path projectSubPath = projectBasePath.relativize(projectRealPath);

            if(hostMount) {
                String projectRealPathStr = projectRealPath.toString();
                sparkOperatorSpecs.addDriverHostPath("gorproject", projectRealPathStr, projectRealPathStr, null, false);
                sparkOperatorSpecs.addExecutorHostPath("gorproject", projectRealPathStr, projectRealPathStr, null, false);
            } else {
                sparkOperatorSpecs.addDriverVolumeClaim("gorproject", "pvc-gor-nfs-v2", projectRealPath.toString(), projectSubPath.toString(), false);
                sparkOperatorSpecs.addExecutorVolumeClaim("gorproject", "pvc-gor-nfs-v2", projectRealPath.toString(), projectSubPath.toString(), false);

                sparkOperatorSpecs.addDriverVolumeClaim("data", "pvc-phenocat-v2", "/mnt/csa/data", "data", true);
                sparkOperatorSpecs.addExecutorVolumeClaim("data", "pvc-phenocat-v2", "/mnt/csa/data", "data", true);

                sparkOperatorSpecs.addDriverVolumeClaim("volumes", "pvc-sm-v2", "/mnt/csa/volumes", "volumes", true);
                sparkOperatorSpecs.addExecutorVolumeClaim("volumes", "pvc-sm-v2", "/mnt/csa/volumes", "volumes", true);
            }

            String[] args = new String[]{uristr, requestId, projectDir, queries, fingerprint, cachefile, jobid};
            List<String> arglist = Arrays.asList(args);
            sparkOperatorSpecs.addConfig("spec.arguments", arglist);

            String sparkApplicationName = "gorquery-" + jobid;
            sparkOperatorSpecs.addConfig("metadata.name", sparkApplicationName);

            for (String config : resourceSplit[1].split(" ")) {
                String[] confSplit = config.split("=");
                try {
                    Integer ii = Integer.parseInt(confSplit[1]);
                    sparkOperatorSpecs.addConfig(confSplit[0], ii);
                } catch (NumberFormatException ne) {
                    sparkOperatorSpecs.addConfig(confSplit[0], confSplit[1]);
                }
            }

            String yaml = getSparkOperatorYaml(projectDir);
            if(debug) {
                runLocal(sparkSession, args);
            } else {
                runYaml(yaml, projectDir, sparkOperatorSpecs);
                waitForSparkApplicationToComplete(gm, sparkApplicationName);
            }
        }
        return cachefilepath;
    }

    /**
     * Keep this for debuging purposes, no kubernetes needed
     * @param sparkSession
     * @param args
     */
    private void runLocal(SparkSession sparkSession, String[] args) {
        String redisUrl = args[0];
        String requestId = args[1];
        String projectDir = args[2];
        String queries = args[3];
        String fingerprints = args[4];
        String cachefiles = args[5];
        String jobids = args[6];
        try(RedisBatchConsumer redisBatchConsumer = new RedisBatchConsumer(sparkSession, redisUrl)) {
            String[] arr = new String[]{queries, fingerprints, projectDir, requestId, jobids, cachefiles};
            List<String[]> lstr = Collections.singletonList(arr);
            Map<String, Future<List<String>>> futMap = redisBatchConsumer.runJobBatch(lstr);
            for(Future<List<String>> f : futMap.values()) {
                f.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new GorSystemException(e);
        }
    }

    public void runYaml(String yaml, String projectroot, SparkOperatorSpecs specs) throws IOException, ApiException {
        if (projectroot == null || projectroot.length() == 0)
            projectroot = Paths.get(".").toAbsolutePath().normalize().toString();
        Map<String, Object> body = loadBody(yaml, projectroot, "", new HashMap<>());
        specs.apply(body);
        apiInstance.createNamespacedCustomObject("sparkoperator.k8s.io", "v1beta2", namespace, "sparkapplications", body, "true", null, null);
    }
}
