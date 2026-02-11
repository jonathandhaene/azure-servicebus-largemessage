/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.config;

import com.azure.servicebus.largemessage.client.AzureServiceBusLargeMessageClient;
import com.azure.servicebus.largemessage.store.BlobNameResolver;
import com.azure.servicebus.largemessage.store.BlobPayloadStore;
import com.azure.servicebus.largemessage.store.MessageBodyReplacer;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Boot auto-configuration for Azure Service Bus Large Message Client.
 * Automatically creates and configures beans when appropriate properties are set.
 */
@Configuration
@EnableConfigurationProperties(LargeMessageClientConfiguration.class)
public class AzureLargeMessageClientAutoConfiguration {

    @Value("${azure.storage.connection-string:}")
    private String storageConnectionString;

    @Value("${azure.storage.container-name:large-messages}")
    private String containerName;

    @Value("${azure.servicebus.connection-string:}")
    private String serviceBusConnectionString;

    @Value("${azure.servicebus.queue-name:my-queue}")
    private String queueName;

    /**
     * Creates a BlobServiceClient bean.
     * Only created when not in receive-only mode.
     * Uses custom StorageConnectionStringProvider if available, otherwise falls back to property.
     *
     * @return the BlobServiceClient instance
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "azure.servicebus.large-message-client.receive-only-mode", havingValue = "false", matchIfMissing = true)
    public BlobServiceClient blobServiceClient(
            @Autowired(required = false) StorageConnectionStringProvider connectionStringProvider) {
        
        // Get connection string from provider if available, otherwise use property
        String connectionString;
        if (connectionStringProvider != null) {
            connectionString = connectionStringProvider.getConnectionString();
        } else {
            connectionString = storageConnectionString;
        }
        
        if (connectionString == null || connectionString.isEmpty()) {
            throw new IllegalStateException(
                "Azure Storage connection string is required. " +
                "Please set azure.storage.connection-string property, " +
                "AZURE_STORAGE_CONNECTION_STRING environment variable, " +
                "or provide a StorageConnectionStringProvider bean."
            );
        }
        return new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();
    }

    /**
     * Creates a BlobPayloadStore bean.
     * Only created when not in receive-only mode.
     *
     * @param blobServiceClient the blob service client
     * @return the BlobPayloadStore instance
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "azure.servicebus.large-message-client.receive-only-mode", havingValue = "false", matchIfMissing = true)
    public BlobPayloadStore blobPayloadStore(BlobServiceClient blobServiceClient, LargeMessageClientConfiguration config) {
        return new BlobPayloadStore(blobServiceClient, containerName, config);
    }

    /**
     * Creates an AzureServiceBusLargeMessageClient bean.
     * Works in both regular mode (with BlobPayloadStore) and receive-only mode (without).
     *
     * @param config       the large message client configuration
     * @return the AzureServiceBusLargeMessageClient instance
     */
    @Bean
    @ConditionalOnMissingBean
    public AzureServiceBusLargeMessageClient azureServiceBusLargeMessageClient(
            LargeMessageClientConfiguration config,
            org.springframework.beans.factory.ObjectProvider<BlobPayloadStore> payloadStoreProvider,
            @Autowired(required = false) BlobNameResolver customBlobNameResolver,
            @Autowired(required = false) MessageBodyReplacer customBodyReplacer,
            @Autowired(required = false) MessageSizeCriteria customMessageSizeCriteria) {
        
        if (serviceBusConnectionString == null || serviceBusConnectionString.isEmpty()) {
            throw new IllegalStateException(
                "Azure Service Bus connection string is required. " +
                "Please set azure.servicebus.connection-string property or AZURE_SERVICEBUS_CONNECTION_STRING environment variable."
            );
        }

        // Set custom blob name resolver if provided
        if (customBlobNameResolver != null) {
            config.setBlobNameResolver(customBlobNameResolver);
        }

        // Set custom body replacer if provided
        if (customBodyReplacer != null) {
            config.setBodyReplacer(customBodyReplacer);
        }

        // Set custom message size criteria if provided
        if (customMessageSizeCriteria != null) {
            config.setMessageSizeCriteria(customMessageSizeCriteria);
        }

        // Get BlobPayloadStore if available (will be null in receive-only mode)
        BlobPayloadStore payloadStore = payloadStoreProvider.getIfAvailable();

        return new AzureServiceBusLargeMessageClient(
                serviceBusConnectionString,
                queueName,
                payloadStore,
                config
        );
    }
}
