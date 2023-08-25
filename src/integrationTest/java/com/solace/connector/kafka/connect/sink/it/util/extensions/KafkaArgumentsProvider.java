package com.solace.connector.kafka.connect.sink.it.util.extensions;

import com.solace.connector.kafka.connect.sink.it.SolaceConnectorDeployment;
import com.solace.connector.kafka.connect.sink.it.TestKafkaProducer;
import com.solace.connector.kafka.connect.sink.it.util.KafkaConnection;
import com.solace.connector.kafka.connect.sink.it.util.extensions.pubsubplus.NetworkPubSubPlusContainerProvider;
import com.solace.connector.kafka.connect.sink.it.util.testcontainers.BitnamiKafkaConnectContainer;
import com.solace.connector.kafka.connect.sink.it.util.testcontainers.ConfluentKafkaConnectContainer;
import com.solace.connector.kafka.connect.sink.it.util.testcontainers.ConfluentKafkaControlCenterContainer;
import com.solace.connector.kafka.connect.sink.it.util.testcontainers.ConfluentKafkaSchemaRegistryContainer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junitpioneer.jupiter.cartesian.CartesianArgumentsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Parameter;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

public class KafkaArgumentsProvider implements ArgumentsProvider,
		CartesianArgumentsProvider<KafkaArgumentsProvider.KafkaContext> {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaArgumentsProvider.class);

	@Override
	public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
		return createKafkaContexts(context).map(Arguments::of);
	}

	@Override
	public Stream<KafkaContext> provideArguments(ExtensionContext context, Parameter parameter) {
		Objects.requireNonNull(parameter.getAnnotation(KafkaSource.class));
		return createKafkaContexts(context);
	}

	private Stream<KafkaContext> createKafkaContexts(ExtensionContext context) {
		KafkaConnection bitnamiCxn = context.getRoot()
				.getStore(KafkaNamespace.BITNAMI.getNamespace())
				.getOrComputeIfAbsent(BitnamiResource.class, c -> {
					LOG.info("Creating Bitnami Kafka");
					BitnamiKafkaConnectContainer container = new BitnamiKafkaConnectContainer()
							.withNetwork(NetworkPubSubPlusContainerProvider.DOCKER_NET);
					if (!container.isCreated()) {
						container.start();
					}
					return new BitnamiResource(container);
				}, BitnamiResource.class)
				.getKafkaConnection();

		KafkaConnection confluentCxn = context.getRoot()
				.getStore(KafkaNamespace.CONFLUENT.getNamespace())
				.getOrComputeIfAbsent(ConfluentResource.class, c -> {
					LOG.info("Creating Confluent Kafka");
					KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka").withTag("7.4.1"))
							.withNetwork(NetworkPubSubPlusContainerProvider.DOCKER_NET)
							.withNetworkAliases("kafka");
					if (!kafkaContainer.isCreated()) {
						kafkaContainer.start();
					}

					ConfluentKafkaSchemaRegistryContainer schemaRegistryContainer = new ConfluentKafkaSchemaRegistryContainer(kafkaContainer)
							.withNetworkAliases("schema-registry");
					if (!schemaRegistryContainer.isCreated()) {
						schemaRegistryContainer.start();
					}

					ConfluentKafkaControlCenterContainer controlCenterContainer = new ConfluentKafkaControlCenterContainer(kafkaContainer, schemaRegistryContainer);
					if (!controlCenterContainer.isCreated()) {
						controlCenterContainer.start();
					}

					ConfluentKafkaConnectContainer connectContainer = new ConfluentKafkaConnectContainer(kafkaContainer, schemaRegistryContainer);
					if (!connectContainer.isCreated()) {
						connectContainer.start();
					}
					return new ConfluentResource(
							new KafkaContainerResource<>(kafkaContainer),
							new KafkaContainerResource<>(schemaRegistryContainer),
							new KafkaContainerResource<>(controlCenterContainer),
							new KafkaContainerResource<>(connectContainer));
				}, ConfluentResource.class)
				.getKafkaConnection();

		return Stream.of(
			createKafkaContext(bitnamiCxn, KafkaNamespace.BITNAMI, context),
			createKafkaContext(confluentCxn, KafkaNamespace.CONFLUENT, context)
		);
	}

	private KafkaContext createKafkaContext(KafkaConnection connection, KafkaNamespace namespace, ExtensionContext context) {
		TestKafkaProducer producer = context.getRoot()
				.getStore(namespace.getNamespace())
				.getOrComputeIfAbsent(ProducerResource.class, c -> {
					TestKafkaProducer newProducer = new TestKafkaProducer(connection.getBootstrapServers(),
							SolaceConnectorDeployment.kafkaTestTopic);
					newProducer.start();
					return new ProducerResource(newProducer);
				}, ProducerResource.class)
				.getProducer();

		AdminClient adminClient = context.getRoot()
				.getStore(namespace.getNamespace())
				.getOrComputeIfAbsent(AdminClientResource.class, c -> {
					Properties properties = new Properties();
					properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connection.getBootstrapServers());
					AdminClient newAdminClient = AdminClient.create(properties);
					return new AdminClientResource(newAdminClient);
				}, AdminClientResource.class)
				.getAdminClient();

		SolaceConnectorDeployment connectorDeployment = context.getRoot()
				.getStore(namespace.getNamespace())
				.getOrComputeIfAbsent(ConnectorDeploymentResource.class, c -> {
					SolaceConnectorDeployment deployment = new SolaceConnectorDeployment(connection, adminClient);
					deployment.waitForConnectorRestIFUp();
					deployment.provisionKafkaTestTopic();
					return new ConnectorDeploymentResource(deployment);
				}, ConnectorDeploymentResource.class)
				.getDeployment();

		return new KafkaContext(namespace, connection, adminClient, connectorDeployment, producer);
	}

	@Target(ElementType.PARAMETER)
	@Retention(RetentionPolicy.RUNTIME)
	@ArgumentsSource(KafkaArgumentsProvider.class)
	public @interface KafkaSource {}

	public static class AutoDeleteSolaceConnectorDeploymentAfterEach implements AfterEachCallback {

		@Override
		public void afterEach(ExtensionContext context) throws Exception {
			for (KafkaNamespace namespace : KafkaNamespace.values()) {
				ConnectorDeploymentResource deploymentResource = context.getRoot()
						.getStore(namespace.getNamespace())
						.get(ConnectorDeploymentResource.class, ConnectorDeploymentResource.class);
				if (deploymentResource != null) {
					deploymentResource.close();
				}
			}
		}
	}

	public static class KafkaContext {
		private final KafkaNamespace namespace;
		private final KafkaConnection connection;
		private final AdminClient adminClient;
		private final SolaceConnectorDeployment solaceConnectorDeployment;
		private final TestKafkaProducer producer;

		private KafkaContext(KafkaNamespace namespace, KafkaConnection connection, AdminClient adminClient,
							 SolaceConnectorDeployment solaceConnectorDeployment, TestKafkaProducer producer) {
			this.namespace = namespace;
			this.connection = connection;
			this.producer = producer;
			this.solaceConnectorDeployment = solaceConnectorDeployment;
			this.adminClient = adminClient;
		}

		public KafkaConnection getConnection() {
			return connection;
		}

		public AdminClient getAdminClient() {
			return adminClient;
		}

		public SolaceConnectorDeployment getSolaceConnectorDeployment() {
			return solaceConnectorDeployment;
		}

		public TestKafkaProducer getProducer() {
			return producer;
		}

		@Override
		public String toString() {
			return namespace.name();
		}
	}

	private static class ProducerResource implements CloseableResource {
		private static final Logger LOG = LoggerFactory.getLogger(ProducerResource.class);
		private final TestKafkaProducer producer;

		private ProducerResource(TestKafkaProducer producer) {
			this.producer = producer;
		}

		public TestKafkaProducer getProducer() {
			return producer;
		}

		@Override
		public void close() {
			LOG.info("Closing Kafka producer");
			producer.close();
		}
	}

	private static class AdminClientResource implements CloseableResource {
		private static final Logger LOG = LoggerFactory.getLogger(AdminClientResource.class);
		private final AdminClient adminClient;

		private AdminClientResource(AdminClient adminClient) {
			this.adminClient = adminClient;
		}

		public AdminClient getAdminClient() {
			return adminClient;
		}

		@Override
		public void close() {
			LOG.info("Closing Kafka admin client");
			adminClient.close();
		}
	}

	private static class ConnectorDeploymentResource implements CloseableResource {
		private static final Logger LOG = LoggerFactory.getLogger(ConnectorDeploymentResource.class);
		private final SolaceConnectorDeployment deployment;

		private ConnectorDeploymentResource(SolaceConnectorDeployment deployment) {
			this.deployment = deployment;
		}

		public SolaceConnectorDeployment getDeployment() {
			return deployment;
		}

		@Override
		public void close() throws IOException, ExecutionException, InterruptedException, TimeoutException {
			LOG.info("Closing Kafka connector deployment");
			deployment.deleteKafkaTestTopic();
			deployment.deleteConnector();
		}
	}

	private static class BitnamiResource extends KafkaContainerResource<BitnamiKafkaConnectContainer> {

		private BitnamiResource(BitnamiKafkaConnectContainer container) {
			super(container);
		}

		public KafkaConnection getKafkaConnection() {
			return new KafkaConnection(getContainer().getBootstrapServers(), getContainer().getConnectUrl(),
					getContainer(), getContainer());
		}
	}

	private static class ConfluentResource implements CloseableResource {
		private final KafkaContainerResource<KafkaContainer> kafka;
		private final KafkaContainerResource<ConfluentKafkaSchemaRegistryContainer> schemaRegistry;
		private final KafkaContainerResource<ConfluentKafkaControlCenterContainer> controlCenter;
		private final KafkaContainerResource<ConfluentKafkaConnectContainer> connect;

		private ConfluentResource(KafkaContainerResource<KafkaContainer> kafka,
								  KafkaContainerResource<ConfluentKafkaSchemaRegistryContainer> schemaRegistry,
								  KafkaContainerResource<ConfluentKafkaControlCenterContainer> controlCenter,
								  KafkaContainerResource<ConfluentKafkaConnectContainer> connect) {
			this.kafka = kafka;
			this.schemaRegistry = schemaRegistry;
			this.controlCenter = controlCenter;
			this.connect = connect;
		}

		public KafkaConnection getKafkaConnection() {
			return new KafkaConnection(kafka.getContainer().getBootstrapServers(),
					connect.getContainer().getConnectUrl(), kafka.container, connect.container);
		}

		public KafkaContainerResource<KafkaContainer> getKafka() {
			return kafka;
		}

		public KafkaContainerResource<ConfluentKafkaConnectContainer> getConnect() {
			return connect;
		}

		@Override
		public void close() {
			connect.close();
			controlCenter.close();
			schemaRegistry.close();
			kafka.close();
		}
	}

	private static class KafkaContainerResource<T extends GenericContainer<?>> implements CloseableResource {
		private static final Logger LOG = LoggerFactory.getLogger(KafkaContainerResource.class);
		private final T container;

		private KafkaContainerResource(T container) {
			this.container = container;
		}

		public T getContainer() {
			return container;
		}

		@Override
		public void close() {
			LOG.info("Closing container {}", container.getContainerName());
			container.close();
		}
	}

	private enum KafkaNamespace {
		BITNAMI, CONFLUENT;

		private final Namespace namespace;

		KafkaNamespace() {
			this.namespace = Namespace.create(KafkaArgumentsProvider.class, name());
		}

		public Namespace getNamespace() {
			return namespace;
		}
	}
}
