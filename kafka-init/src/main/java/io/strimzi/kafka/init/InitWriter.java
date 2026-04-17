/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.kafka.listener.NodeAddressType;
import io.strimzi.operator.common.model.NodeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Collects and writes the configuration collected in the init container
 */
public class InitWriter {
    private static final Logger LOGGER = LogManager.getLogger(InitWriter.class);

    private final KubernetesClient client;
    private final InitWriterConfig config;

    protected final static String FILE_RACK_ID = "rack.id";
    protected final static String FILE_EXTERNAL_ADDRESS = "external.address";
    protected final static String FILE_JAAS_CONF = "jaas.conf";

    /**
     * Constructs the InitWriter
     *
     * @param client    Kubernetes client
     * @param config    InitWriter configuration
     */
    public InitWriter(KubernetesClient client, InitWriterConfig config) {
        this.client = client;
        this.config = config;
    }

    /**
     * Write the rack-id
     *
     * @return if the operation was executed successfully
     */
    public boolean writeRack() {

        Map<String, String> nodeLabels = client.nodes().withName(config.getNodeName()).get().getMetadata().getLabels();
        LOGGER.info("NodeLabels = {}", nodeLabels);
        String rackId = nodeLabels.get(config.getRackTopologyKey());
        LOGGER.info("Rack: {} = {}", config.getRackTopologyKey(), rackId);

        if (rackId == null) {
            LOGGER.error("Node {} doesn't have the label {} for getting the rackid",
                    config.getNodeName(), config.getRackTopologyKey());
            return false;
        }

        return write(FILE_RACK_ID, rackId);
    }

    /**
     * Write the external address of this node
     *
     * @return if the operation was executed successfully
     */
    public boolean writeExternalAddress() {
        List<NodeAddress> addresses = client.nodes().withName(config.getNodeName()).get().getStatus().getAddresses();
        StringBuilder externalAddresses = new StringBuilder();

        String address = NodeUtils.findAddress(addresses, null);

        if (address == null) {
            LOGGER.error("External address not found");
            return false;
        } else  {
            LOGGER.info("Default External address found {}", address);
            externalAddresses.append(externalAddressExport(null, address));
        }

        for (NodeAddressType type : NodeAddressType.values())   {
            address = NodeUtils.findAddress(addresses, type);
            LOGGER.info("External {} address found {}", type.toValue(), address);
            externalAddresses.append(externalAddressExport(type, address));
        }

        return write(FILE_EXTERNAL_ADDRESS, externalAddresses.toString());
    }

    /**
     * Formats address type and address into shell export command for environment variable
     *
     * @param type      Type of the address. Use null for default address
     * @param address   Address for given type
     * @return          String with the shell command
     */
    private String externalAddressExport(NodeAddressType type, String address) {
        String envVar;

        if (type != null) {
            envVar = String.format("nodeport.%s.address", type.toValue().toLowerCase(Locale.ENGLISH));
        } else {
            envVar = "nodeport.default.address";
        }

        return String.format("%s=%s", envVar, address) + System.lineSeparator();
    }

    /**
     * Write the fwss user secrets to jaas.conf
     *
     * @param secretList   List of fwss secrets in the namespace
     * @return if the operation was executed successfully
     */
    public boolean writeFwssSecretsToJaasConf(SecretList secretList) {

        if (secretList.getItems().isEmpty()) {
            // no fwss labeled secrets, then exit
            LOGGER.error("SecretList is empty");
            return false;
        }
        List<Secret> secrets = secretList.getItems();
        List<Secret> kafkaSecret = secrets.stream()
                .filter(secret -> secret.getMetadata().getName().equals(config.getFwssSecretName()))
                .toList();
        if (kafkaSecret.isEmpty()) {
            // no fwss secrets with the give name, then exit
            LOGGER.error("No secrets with name '{}' found.", config.getFwssSecretName());
            return false;
        }

        Map.Entry<String, String> adminNameAndSecret = kafkaSecret.get(0).getData().entrySet().iterator().next();
        String kafkaFwssJaasConfig = new String(java.util.Base64.getDecoder().decode(adminNameAndSecret.getValue())).trim();
        if (!isValidJSON(kafkaFwssJaasConfig)) {
            LOGGER.error("Invalid JSON format for KafkaFwssJaasConfig");
            return false;
        }
        return configConvertAndWrite(kafkaFwssJaasConfig);

    }

    /**
     * Convert fwss jaas secret to jaas config
     *
     * @param kafkaFwssJaasConfig   Information to be written
     * @return              true if conversion succeeded, false otherwise
     */
    public boolean configConvertAndWrite(String kafkaFwssJaasConfig) {
     
        if (kafkaFwssJaasConfig.isEmpty()) {
            LOGGER.error("KafkaFwssJaasConfig is empty");
            return false;
        }
        // Removing braces and extra whitespace, then splitting by commas
        String input = kafkaFwssJaasConfig.replaceAll("[{}\"]", "").trim();
        String[] pairs = input.split(",");

        // Initialize StringBuilder for formatted output
        StringBuilder jaasConfig = new StringBuilder();
        jaasConfig.append("KafkaServer {\n");
        jaasConfig.append("  org.apache.kafka.common.security.plain.PlainLoginModule required\n");

        // Process each key-value pair
        for (String pair : pairs) {
            String[] keyValue = pair.trim().split(":");
            String key = keyValue[0].trim();
            String username = keyValue[1].trim();
            String password = keyValue[2].trim();

            if (key.equals("kafka_admin")) {
                if (username.isEmpty() || password.isEmpty()) {
                    LOGGER.error("Either of admin username or password is empty");
                    return false;
                }
                jaasConfig.append("  username=\"").append(username).append("\"\n");
                jaasConfig.append("  password=\"").append(password).append("\"\n");
                jaasConfig.append("  user_").append(username).append("=\"").append(password).append("\"\n");
            } else if (key.startsWith("kafka_user")) {
                jaasConfig.append("  user_").append(username).append("=\"").append(password).append("\"\n");
            }
        }
        // Replace the last newline character jaasConfig with ";"
        jaasConfig.setCharAt(jaasConfig.length() - 1, ';');
        jaasConfig.append("\n};");

        return write(FILE_JAAS_CONF, jaasConfig.toString());
    }

    /**
     * Checks if the provided string is a valid JSON
     *
     * @param jsonString   string to be checked
     * @return             true if valid json succeeded, false otherwise
     */
    private boolean isValidJSON(String jsonString) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.readTree(jsonString);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Write provided information into a file
     *
     * @param file          Target file
     * @param information   Information to be written
     * @return              true if write succeeded, false otherwise
     */
    private boolean write(String file, String information) {
        boolean isWritten;

        try (PrintWriter writer = new PrintWriter(config.getInitFolder() + "/" + file, StandardCharsets.UTF_8)) {
            writer.write(information);

            if (writer.checkError())    {
                LOGGER.error("Failed to write the information {} to file {}", information, file);
                isWritten = false;
            } else {
                if (file.equals(FILE_JAAS_CONF)) {
                    // to mask jaas secrets
                    LOGGER.info("Jaas information string of length {} written successfully to file {}", information.length(), file);
                } else {
                    LOGGER.info("Information {} written successfully to file {}", information, file);
                }
                isWritten = true;
            }
        } catch (IOException e) {
            LOGGER.error("Error writing the information {} to file {}", information, file, e);
            isWritten = false;
        }

        return isWritten;
    }
}
