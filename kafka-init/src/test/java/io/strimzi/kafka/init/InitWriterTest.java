/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.api.model.NodeAddressBuilder;
import io.fabric8.kubernetes.api.model.NodeStatus;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InitWriterTest {
    @TempDir
    public File tempDir;

    private static final Map<String, String> ENV_VARS = Map.of(
            InitWriterConfig.NODE_NAME.key(), "localhost",
            InitWriterConfig.RACK_TOPOLOGY_KEY.key(), "failure-domain.beta.kubernetes.io/zone",
            InitWriterConfig.EXTERNAL_ADDRESS.key(), "true"
    );
    // metadata labels related to the Kubernetes cluster node
    private static final Map<String, String> LABELS = Map.of(
            "beta.kubernetes.io/arch", "amd64",
            "beta.kubernetes.io", "linux",
            "kubernetes.io/hostname", "localhost",
            "failure-domain.beta.kubernetes.io/zone", "eu-zone1"
    );
    private static final List<NodeAddress> ADDRESSES = List.of(
            new NodeAddressBuilder().withType("ExternalDNS").withAddress("my.external.address").build(),
            new NodeAddressBuilder().withType("InternalDNS").withAddress("my.internal.address").build(),
            new NodeAddressBuilder().withType("InternalIP").withAddress("192.168.2.94").build()
    );

    @Test
    public void testWriteRackId() throws IOException {
        // create and configure (env vars) the path to the rack-id file
        File kafkaFolder = new File(tempDir.getPath() + "opt/kafka");
        String rackFolder = kafkaFolder.getAbsolutePath() + "/rack";
        new File(rackFolder).mkdirs();

        Map<String, String> envVars = new HashMap<>(ENV_VARS);
        envVars.put(InitWriterConfig.INIT_FOLDER.key(), rackFolder);

        InitWriterConfig config = InitWriterConfig.fromMap(envVars);

        KubernetesClient client = mockKubernetesClient(config.getNodeName(), LABELS, List.of());

        InitWriter writer = new InitWriter(client, config);
        assertThat(writer.writeRack(), is(true));
        assertThat(readFile(rackFolder + "/rack.id"), is("eu-zone1"));
    }

    @Test
    public void testWriteExternalAddress() throws IOException {
        // create and configure (env vars) the path to the rack-id file
        File kafkaFolder = new File(tempDir.getPath(), "/opt/kafka");
        String addressFolder = kafkaFolder.getAbsolutePath() + "/external.address";
        new File(addressFolder).mkdirs();

        Map<String, String> envVars = new HashMap<>(ENV_VARS);
        envVars.put(InitWriterConfig.INIT_FOLDER.key(), addressFolder);

        InitWriterConfig config = InitWriterConfig.fromMap(envVars);

        KubernetesClient client = mockKubernetesClient(config.getNodeName(), Map.of(), ADDRESSES);

        InitWriter writer = new InitWriter(client, config);
        assertThat(writer.writeExternalAddress(), is(true));
        assertThat(readFile(addressFolder + "/external.address"), is(
                "nodeport.default.address=my.external.address\n" +
                "nodeport.externalip.address=my.external.address\n" +
                "nodeport.externaldns.address=my.external.address\n" +
                "nodeport.internalip.address=192.168.2.94\n" +
                "nodeport.internaldns.address=my.internal.address\n" +
                "nodeport.hostname.address=my.external.address\n"));
    }

    @Test
    public void testWriteJaasConfig() throws IOException {

        // create and configure (env vars) the path to the rack-id file
        File kafkaFolder = new File(tempDir.getPath(), "/opt/kafka");
        String jaasFolder = kafkaFolder.getAbsolutePath() + "/jaas.conf";
        new File(jaasFolder).mkdirs();

        Map<String, String> envVars = new HashMap<>(ENV_VARS);
        envVars.put(InitWriterConfig.INIT_FOLDER.key(), jaasFolder);

        InitWriterConfig config = InitWriterConfig.fromMap(envVars);

        KubernetesClient client = mockKubernetesClient(config.getNodeName(), Map.of(), ADDRESSES);

        InitWriter writer = new InitWriter(client, config);
        assertThat(writer.writeExternalAddress(), is(true));

        writer.configConvertAndWrite("{\n" +
                "  \"kafka_admin\": \"adminusername:adminpassword\",\n" +
                "  \"zookeeper_user\": \"zkuser:zkpassword\",\n" +
                "  \"kafka_user_1\": \"kafkauser1:kafkauser1password\",\n" +
                "  \"kafka_user_2\": \"kafkauser2:kafkauser2password\",\n" +
                "  \"kafka_user_3\": \"kafkauser3:kafkauser3password\"\n" +
                "}");
        assertThat(readFile(jaasFolder + "/jaas.conf"), is("KafkaServer {\n" +
                "  org.apache.kafka.common.security.plain.PlainLoginModule required\n" +
                "  username=\"adminusername\"\n" +
                "  password=\"adminpassword\"\n" +
                "  user_adminusername=\"adminpassword\"\n" +
                "  user_kafkauser1=\"kafkauser1password\"\n" +
                "  user_kafkauser2=\"kafkauser2password\"\n" +
                "  user_kafkauser3=\"kafkauser3password\";\n" +
                "};"));
    }

    @Test
    public void testWriteRackFailWithMissingKubernetesZoneLabel() {
        // the cluster node will not have the requested label
        Map<String, String> labels = new HashMap<>(LABELS);
        labels.remove("failure-domain.beta.kubernetes.io/zone");

        InitWriterConfig config = InitWriterConfig.fromMap(ENV_VARS);

        KubernetesClient client = mockKubernetesClient(config.getNodeName(), labels, List.of());

        InitWriter writer = new InitWriter(client, config);
        assertThat(writer.writeRack(), is(false));
    }

    @Test
    public void testWriteRackFailsWhenInitFolderDoesNotExist() {
        // specify a not existing folder for emulating IOException in the rack writer
        Map<String, String> envVars = new HashMap<>(ENV_VARS);
        envVars.put(InitWriterConfig.INIT_FOLDER.key(), "/no-folder");

        InitWriterConfig config = InitWriterConfig.fromMap(envVars);

        KubernetesClient client = mockKubernetesClient(config.getNodeName(), LABELS, ADDRESSES);

        InitWriter writer = new InitWriter(client, config);
        assertThat(writer.writeRack(), is(false));
    }

    private String readFile(String file) throws IOException {
        return new String(Files.readAllBytes(Paths.get(file)));
    }

    /**
     * Mock a Kubernetes client for getting cluster node information
     *
     * @param nodeName cluster node name
     * @param labels metadata labels to be returned for the provided cluster node name
     * @return mocked Kubernetes client
     */
    private KubernetesClient mockKubernetesClient(String nodeName, Map<String, String> labels, List<NodeAddress> addresses) {
        KubernetesClient client = mock(KubernetesClient.class);
        NonNamespaceOperation mockNodes = mock(NonNamespaceOperation.class);
        Resource mockResource = mock(Resource.class);
        Node mockNode = mock(Node.class);
        ObjectMeta mockNodeMetadata = mock(ObjectMeta.class);
        NodeStatus mockNodeStatus = mock(NodeStatus.class);

        when(client.nodes()).thenReturn(mockNodes);
        when(mockNodes.withName(nodeName)).thenReturn(mockResource);
        when(mockResource.get()).thenReturn(mockNode);
        when(mockNode.getMetadata()).thenReturn(mockNodeMetadata);
        when(mockNodeMetadata.getLabels()).thenReturn(labels);
        when(mockNode.getStatus()).thenReturn(mockNodeStatus);
        when(mockNodeStatus.getAddresses()).thenReturn(addresses);

        return client;
    }
}
