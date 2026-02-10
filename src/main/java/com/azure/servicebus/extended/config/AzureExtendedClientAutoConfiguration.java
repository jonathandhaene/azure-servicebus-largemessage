package com.azure.servicebus.extended.config;

import com.azure.servicebus.extended.client.AzureServiceBusExtendedAsyncClient;
import com.azure.servicebus.extended.client.AzureServiceBusExtendedClient;
import com.azure.servicebus.extended.store.BlobPayloadStore;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Boot auto-configuration for Azure Service Bus Extended Client.
 * Automatically creates and configures beans when appropriate properties are set.
 */
@Configuration
@EnableConfigurationProperties(ExtendedClientConfiguration.class)
public class AzureExtendedClientAutoConfiguration {

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
     *
     * @return the BlobServiceClient instance
     */
    @Bean
    @ConditionalOnMissingBean
    public BlobServiceClient blobServiceClient() {
        if (storageConnectionString == null || storageConnectionString.isEmpty()) {
            throw new IllegalStateException(
                "Azure Storage connection string is required. " +
                "Please set azure.storage.connection-string property or AZURE_STORAGE_CONNECTION_STRING environment variable."
            );
        }
        return new BlobServiceClientBuilder()
                .connectionString(storageConnectionString)
                .buildClient();
    }

    /**
     * Creates a BlobPayloadStore bean.
     *
     * @param blobServiceClient the blob service client
     * @param config the extended client configuration
     * @return the BlobPayloadStore instance
     */
    @Bean
    @ConditionalOnMissingBean
    public BlobPayloadStore blobPayloadStore(BlobServiceClient blobServiceClient, ExtendedClientConfiguration config) {
        BlobPayloadStore store = new BlobPayloadStore(blobServiceClient, containerName);
        
        // Configure blob access tier if specified
        if (config.getBlobAccessTier() != null && !config.getBlobAccessTier().isEmpty()) {
            store.setBlobAccessTier(config.getBlobAccessTier());
        }
        
        // Configure encryption if specified
        if (config.getEncryption() != null && config.getEncryption().isEncryptionEnabled()) {
            store.setEncryptionConfig(config.getEncryption());
        }
        
        // Configure ignore payload not found
        store.setIgnorePayloadNotFound(config.isIgnorePayloadNotFound());
        
        return store;
    }

    /**
     * Creates an AzureServiceBusExtendedClient bean.
     *
     * @param payloadStore the blob payload store
     * @param config       the extended client configuration
     * @return the AzureServiceBusExtendedClient instance
     */
    @Bean
    @ConditionalOnMissingBean
    public AzureServiceBusExtendedClient azureServiceBusExtendedClient(
            BlobPayloadStore payloadStore,
            ExtendedClientConfiguration config) {
        
        if (serviceBusConnectionString == null || serviceBusConnectionString.isEmpty()) {
            throw new IllegalStateException(
                "Azure Service Bus connection string is required. " +
                "Please set azure.servicebus.connection-string property or AZURE_SERVICEBUS_CONNECTION_STRING environment variable."
            );
        }

        return new AzureServiceBusExtendedClient(
                serviceBusConnectionString,
                queueName,
                payloadStore,
                config
        );
    }

    /**
     * Creates an AzureServiceBusExtendedAsyncClient bean.
     *
     * @param payloadStore the blob payload store
     * @param config       the extended client configuration
     * @return the AzureServiceBusExtendedAsyncClient instance
     */
    @Bean
    @ConditionalOnMissingBean
    public AzureServiceBusExtendedAsyncClient azureServiceBusExtendedAsyncClient(
            BlobPayloadStore payloadStore,
            ExtendedClientConfiguration config) {
        
        if (serviceBusConnectionString == null || serviceBusConnectionString.isEmpty()) {
            throw new IllegalStateException(
                "Azure Service Bus connection string is required. " +
                "Please set azure.servicebus.connection-string property or AZURE_SERVICEBUS_CONNECTION_STRING environment variable."
            );
        }

        return new AzureServiceBusExtendedAsyncClient(
                serviceBusConnectionString,
                queueName,
                payloadStore,
                config
        );
    }
}
