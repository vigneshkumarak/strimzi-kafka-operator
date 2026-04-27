/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.common.OperatorKubernetesClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The Main class used to run the init container
 */
public class Main {
    private static final Logger LOGGER = LogManager.getLogger(Main.class);

    /**
     * The main method
     *
     * @param args  Array with arguments form the command line
     */
    public static void main(String[] args) {
        LOGGER.info("FW Custom Init-kafka is starting");
        final String strimziVersion = Main.class.getPackage().getImplementationVersion();
        LOGGER.info("Init-kafka {} is starting", strimziVersion);
        InitWriterConfig config = InitWriterConfig.fromMap(System.getenv());

        final KubernetesClient client = new OperatorKubernetesClientBuilder("strimzi-kafka-init", strimziVersion).build();

        LOGGER.info("Init-kafka started with config: {}", config);

        InitWriter writer = new InitWriter(client, config);

        if (config.getIfAuthenticationIsSaslScramAndPlain()) {
            String namespace = client.getNamespace();

            if (namespace == null || namespace.isEmpty()) {
                LOGGER.error("Namespace is null or empty");
                System.exit(1);
            }

            // List all secrets in the current namespace
            SecretList secretList = client.secrets().inNamespace(namespace)
                    .withLabel(config.getFwssLabelKey(), config.getFwssLabelValue())
                    .list();
            LOGGER.info("Process Secrets");
            if (!writer.writeFwssSecretsToJaasConf(secretList)) {
                LOGGER.error("Failed to write fwss secrets");
                System.exit(1);
            }
        }

        if (config.getRackTopologyKey() != null) {
            if (!writer.writeRack()) {
                LOGGER.error("Failed to write rack topology");
                System.exit(1);
            }
        }

        if (config.isExternalAddress()) {
            if (!writer.writeExternalAddress()) {
                LOGGER.error("Failed to write external address");
                System.exit(1);
            }
        }

        client.close();
    }
}
