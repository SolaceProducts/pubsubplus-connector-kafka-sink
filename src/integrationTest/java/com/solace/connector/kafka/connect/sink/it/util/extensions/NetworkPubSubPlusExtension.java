package com.solace.connector.kafka.connect.sink.it.util.extensions;

import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.testcontainer.PubSubPlusContainer;
import org.testcontainers.containers.Network;

public class NetworkPubSubPlusExtension extends PubSubPlusExtension {
	private static final Network DOCKER_NET = Network.newNetwork();
	private static final String DOCKER_NET_PUBSUB_ALIAS = "solace-pubsubplus";

	public NetworkPubSubPlusExtension() {
		super(() -> new PubSubPlusContainer()
				.withNetwork(DOCKER_NET)
				.withNetworkAliases(DOCKER_NET_PUBSUB_ALIAS));
	}

	public Network getDockerNetwork() {
		return DOCKER_NET;
	}

	public String getNetworkAlias() {
		return DOCKER_NET_PUBSUB_ALIAS;
	}
}
