package com.solace.connector.kafka.connect.sink.it;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.solace.connector.kafka.connect.sink.SolaceSinkConnector;
import com.solace.connector.kafka.connect.sink.VersionUtil;
import com.solace.connector.kafka.connect.sink.it.util.KafkaConnection;
import com.solacesystems.jcsmp.JCSMPProperties;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SolaceConnectorDeployment implements TestConstants {

  static Logger logger = LoggerFactory.getLogger(SolaceConnectorDeployment.class.getName());

  static String kafkaTestTopic = KAFKA_SINK_TOPIC + "-" + Instant.now().getEpochSecond();
  OkHttpClient client = new OkHttpClient();
  AdminClient adminClient;
  private final KafkaConnection kafkaConnection;
  private final String pubSubPlusHost;
  private final JCSMPProperties jcsmpProperties;

  public SolaceConnectorDeployment(KafkaConnection kafkaConnection, String pubSubPlusHost, JCSMPProperties jcsmpProperties) {
    this.kafkaConnection = kafkaConnection;
    this.pubSubPlusHost = pubSubPlusHost;
    this.jcsmpProperties = jcsmpProperties;
  }

  public void waitForConnectorRestIFUp() {
    Request request = new Request.Builder().url(kafkaConnection.getConnectUrl() + "/connector-plugins").build();
    boolean success = false;
    do {
      try {
        Thread.sleep(1000L);
        try (Response response = client.newCall(request).execute()) {
          success = response.isSuccessful();
        }
      } catch (IOException | InterruptedException e) {
        // Continue looping
      }
    } while (!success);
  }

  public void startAdminClient() {
    if (adminClient == null) {
      Properties properties = new Properties();
      properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnection.getBootstrapServers());
      adminClient = AdminClient.create(properties);
    }
  }

  public void closeAdminClient() {
    if (adminClient != null) {
      adminClient.close();
      adminClient = null;
    }
  }

  public void provisionKafkaTestTopic() {
    // Create a new kafka test topic to use
    NewTopic newTopic = new NewTopic(kafkaTestTopic, 1, (short) 1); // new NewTopic(topicName, numPartitions,
                                                                    // replicationFactor)
    List<NewTopic> newTopics = new ArrayList<NewTopic>();
    newTopics.add(newTopic);
    adminClient.createTopics(newTopics);
  }

  public void deleteKafkaTestTopic() throws ExecutionException, InterruptedException, TimeoutException {
    DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton(kafkaTestTopic));
    result.all().get(1, TimeUnit.MINUTES);
  }

  void startConnector() {
    startConnector(null); // Defaults only, no override
  }

  void startConnector(Properties props) {
    startConnector(props, false);
  }

  void startConnector(Properties props, boolean expectStartFail) {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    String configJson = null;
    // Prep config files
    try {
      // Configure .json connector params
      File jsonFile = new File(
          UNZIPPEDCONNECTORDESTINATION + "/" + Tools.getUnzippedConnectorDirName() + "/" + CONNECTORJSONPROPERTIESFILE);
      String jsonString = FileUtils.readFileToString(jsonFile);
      JsonElement jtree = new JsonParser().parse(jsonString);
      JsonElement jconfig = jtree.getAsJsonObject().get("config");
      JsonObject jobject = jconfig.getAsJsonObject();
      // Set properties defaults
      jobject.addProperty("sol.host", pubSubPlusHost);
      jobject.addProperty("sol.username", jcsmpProperties.getStringProperty(JCSMPProperties.USERNAME));
      jobject.addProperty("sol.password", jcsmpProperties.getStringProperty(JCSMPProperties.PASSWORD));
      jobject.addProperty("sol.vpn_name", jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME));
      jobject.addProperty("topics", kafkaTestTopic);
      jobject.addProperty("sol.topics", SOL_TOPICS);
      jobject.addProperty("sol.autoflush.size", "1");
      jobject.addProperty("sol.message_processor_class", CONN_MSGPROC_CLASS);
      jobject.addProperty("sol.kafka_message_key", CONN_KAFKA_MSGKEY);
      jobject.addProperty("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
      jobject.addProperty("key.converter", "org.apache.kafka.connect.storage.StringConverter");
      // Override properties if provided
      if (props != null) {
        props.forEach((key, value) -> {
          jobject.addProperty((String) key, (String) value);
        });
      }
      configJson = gson.toJson(jtree);
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Configure and start the solace connector
    try {
      // check presence of Solace plugin: curl
      // http://18.218.82.209:8083/connector-plugins | jq
      Request request = new Request.Builder().url(kafkaConnection.getConnectUrl() + "/connector-plugins").build();
      try (Response response = client.newCall(request).execute()) {
        assertTrue(response.isSuccessful());
        JsonArray results = responseBodyToJson(response.body()).getAsJsonArray();
        logger.info("Available connector plugins: " + gson.toJson(results));
        boolean hasConnector = false;
        for (Iterator<JsonElement> resultsIter = results.iterator(); !hasConnector && resultsIter.hasNext();) {
          JsonObject connectorPlugin = resultsIter.next().getAsJsonObject();
          if (connectorPlugin.get("class").getAsString().equals(SolaceSinkConnector.class.getName())) {
            hasConnector = true;
            assertEquals("sink", connectorPlugin.get("type").getAsString());
            assertEquals(VersionUtil.getVersion(), connectorPlugin.get("version").getAsString());
          }
        }
        assertTrue(hasConnector, String.format("Could not find connector %s : %s",
                SolaceSinkConnector.class.getName(), gson.toJson(results)));
      }

      // Delete a running connector, if any
      deleteConnector();

      // configure plugin: curl -X POST -H "Content-Type: application/json" -d
      // @solace_source_properties.json http://18.218.82.209:8083/connectors
      Request configrequest = new Request.Builder().url(kafkaConnection.getConnectUrl() + "/connectors")
          .post(RequestBody.create(configJson, MediaType.parse("application/json"))).build();
      try (Response configresponse = client.newCall(configrequest).execute()) {
        // if (!configresponse.isSuccessful()) throw new IOException("Unexpected code "
        // + configresponse);
        logger.info("Connector config results: " + gson.toJson(responseBodyToJson(configresponse.body())));
      }
      // check success
      AtomicReference<JsonObject> statusResponse = new AtomicReference<>(new JsonObject());
      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        JsonObject connectorStatus;
        do {
          connectorStatus = getConnectorStatus();
          statusResponse.set(connectorStatus);
        } while (!(expectStartFail ? "FAILED" : "RUNNING").equals(Optional.ofNullable(connectorStatus)
                .map(a -> a.getAsJsonArray("tasks"))
                .map(a -> a.size() > 0 ? a.get(0) : null)
                .map(JsonElement::getAsJsonObject)
                .map(a -> a.get("state"))
                .map(JsonElement::getAsString)
                .orElse("")));
      }, () -> "Timed out while waiting for connector to start: " + gson.toJson(statusResponse.get()));
      Thread.sleep(5000); // Give some time to start
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public void deleteConnector() throws IOException {
    Request request = new Request.Builder()
            .url(kafkaConnection.getConnectUrl() + "/connectors/solaceSinkConnector").delete().build();
    try (Response response = client.newCall(request).execute()) {
      logger.info("Delete response: " + response);
    }
  }

  public JsonObject getConnectorStatus() {
    Request request = new Request.Builder()
            .url(kafkaConnection.getConnectUrl() + "/connectors/solaceSinkConnector/status").build();
    return assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
      while (true) {
        try (Response response = client.newCall(request).execute()) {
          if (!response.isSuccessful()) {
            continue;
          }

          return responseBodyToJson(response.body()).getAsJsonObject();
        }
      }
    });
  }

  private JsonElement responseBodyToJson(ResponseBody responseBody) {
    return Optional.ofNullable(responseBody)
            .map(ResponseBody::charStream)
            .map(s -> new JsonParser().parse(s))
            .orElseGet(JsonObject::new);
  }

}
