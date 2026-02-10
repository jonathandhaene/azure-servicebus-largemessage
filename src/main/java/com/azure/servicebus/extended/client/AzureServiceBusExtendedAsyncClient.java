package com.azure.servicebus.extended.client;

import com.azure.messaging.servicebus.*;
import com.azure.servicebus.extended.config.ExtendedClientConfiguration;
import com.azure.servicebus.extended.model.BlobPointer;
import com.azure.servicebus.extended.model.ExtendedServiceBusMessage;
import com.azure.servicebus.extended.store.BlobPayloadStore;
import com.azure.servicebus.extended.util.ApplicationPropertyValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Azure Service Bus Extended Async Client - async version of the extended client.
 * Uses reactive programming with Reactor (Mono/Flux) for non-blocking operations.
 * This is the Azure equivalent of Amazon SQS Extended Async Client.
 */
public class AzureServiceBusExtendedAsyncClient implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(AzureServiceBusExtendedAsyncClient.class);

    private final ServiceBusSenderAsyncClient senderClient;
    private final ServiceBusReceiverAsyncClient receiverClient;
    private final BlobPayloadStore payloadStore;
    private final ExtendedClientConfiguration config;

    /**
     * Creates a new async extended client with connection string.
     *
     * @param connectionString the Service Bus connection string
     * @param queueName        the queue name
     * @param payloadStore     the blob payload store
     * @param config           the extended client configuration
     */
    public AzureServiceBusExtendedAsyncClient(
            String connectionString,
            String queueName,
            BlobPayloadStore payloadStore,
            ExtendedClientConfiguration config) {
        this.payloadStore = payloadStore;
        this.config = config;

        ServiceBusClientBuilder builder = new ServiceBusClientBuilder()
                .connectionString(connectionString);

        this.senderClient = builder.sender()
                .queueName(queueName)
                .buildAsyncClient();

        this.receiverClient = builder.receiver()
                .queueName(queueName)
                .buildAsyncClient();

        logger.info("AzureServiceBusExtendedAsyncClient initialized for queue: {}", queueName);
    }

    /**
     * Creates a new async extended client with pre-built clients (for testing).
     *
     * @param senderClient   the Service Bus async sender client
     * @param receiverClient the Service Bus async receiver client
     * @param payloadStore   the blob payload store
     * @param config         the extended client configuration
     */
    public AzureServiceBusExtendedAsyncClient(
            ServiceBusSenderAsyncClient senderClient,
            ServiceBusReceiverAsyncClient receiverClient,
            BlobPayloadStore payloadStore,
            ExtendedClientConfiguration config) {
        this.senderClient = senderClient;
        this.receiverClient = receiverClient;
        this.payloadStore = payloadStore;
        this.config = config;
        logger.info("AzureServiceBusExtendedAsyncClient initialized with provided clients");
    }

    /**
     * Sends a message asynchronously.
     *
     * @param messageBody the message body to send
     * @return Mono that completes when message is sent
     */
    public Mono<Void> sendMessage(String messageBody) {
        return sendMessage(messageBody, new HashMap<>());
    }

    /**
     * Sends a message asynchronously with application properties.
     *
     * @param messageBody           the message body to send
     * @param applicationProperties custom application properties
     * @return Mono that completes when message is sent
     */
    public Mono<Void> sendMessage(String messageBody, Map<String, Object> applicationProperties) {
        return Mono.fromCallable(() -> prepareMessage(messageBody, applicationProperties))
                .flatMap(message -> senderClient.sendMessage(message))
                .doOnSuccess(v -> logger.debug("Message sent successfully"))
                .doOnError(e -> logger.error("Failed to send message", e))
                .onErrorMap(e -> new RuntimeException("Failed to send message", e));
    }

    /**
     * Sends multiple messages asynchronously in a batch.
     * Each message is evaluated individually for blob offloading.
     *
     * @param messageBodies list of message bodies to send
     * @return Mono that completes when all messages are sent
     */
    public Mono<Void> sendMessageBatch(List<String> messageBodies) {
        return sendMessageBatch(messageBodies, new HashMap<>());
    }

    /**
     * Sends multiple messages asynchronously in a batch with common properties.
     *
     * @param messageBodies list of message bodies to send
     * @param commonProperties application properties to apply to all messages
     * @return Mono that completes when all messages are sent
     */
    public Mono<Void> sendMessageBatch(List<String> messageBodies, Map<String, Object> commonProperties) {
        return Flux.fromIterable(messageBodies)
                .map(body -> prepareMessage(body, commonProperties))
                .collectList()
                .flatMap(messages -> senderClient.sendMessages(messages))
                .doOnSuccess(v -> logger.debug("Message batch sent successfully"))
                .doOnError(e -> logger.error("Failed to send message batch", e))
                .onErrorMap(e -> new RuntimeException("Failed to send message batch", e));
    }

    /**
     * Receives messages asynchronously from the queue.
     *
     * @param maxMessages maximum number of messages to receive
     * @return Flux of extended Service Bus messages with resolved payloads
     */
    public Flux<ExtendedServiceBusMessage> receiveMessages(int maxMessages) {
        return receiverClient.receiveMessages()
                .take(maxMessages)
                .map(this::processReceivedMessage)
                .doOnError(e -> logger.error("Failed to receive messages", e));
    }

    /**
     * Deletes a blob payload asynchronously.
     *
     * @param message the extended Service Bus message
     * @return Mono that completes when deletion is done
     */
    public Mono<Void> deletePayload(ExtendedServiceBusMessage message) {
        return Mono.fromRunnable(() -> {
            if (!config.isCleanupBlobOnDelete()) {
                logger.debug("Blob cleanup is disabled. Skipping deletion.");
                return;
            }

            if (!message.isPayloadFromBlob() || message.getBlobPointer() == null) {
                logger.debug("Message not from blob or no blob pointer. Skipping deletion.");
                return;
            }

            try {
                logger.debug("Deleting blob payload: {}", message.getBlobPointer());
                payloadStore.deletePayload(message.getBlobPointer());
                logger.debug("Blob payload deleted successfully");
            } catch (Exception e) {
                logger.error("Failed to delete blob payload", e);
                // Don't throw - cleanup failure shouldn't break processing
            }
        });
    }

    /**
     * Deletes blob payloads for a batch of messages asynchronously.
     *
     * @param messages list of extended Service Bus messages
     * @return Mono that completes when all deletions are processed
     */
    public Mono<Void> deletePayloadBatch(List<ExtendedServiceBusMessage> messages) {
        return Flux.fromIterable(messages)
                .flatMap(this::deletePayload)
                .then()
                .doOnSuccess(v -> logger.debug("Batch delete completed"))
                .doOnError(e -> logger.error("Failed to delete payload batch", e));
    }

    /**
     * Renews the lock on a message asynchronously.
     *
     * @param message the received message to renew lock for
     * @return Mono that completes when lock is renewed
     */
    public Mono<Void> renewMessageLock(ServiceBusReceivedMessage message) {
        return receiverClient.renewMessageLock(message)
                .doOnSuccess(v -> logger.debug("Message lock renewed for: {}", message.getMessageId()))
                .doOnError(e -> logger.error("Failed to renew message lock", e))
                .then();
    }

    /**
     * Renews locks for a batch of messages asynchronously.
     *
     * @param messages list of received messages to renew locks for
     * @return Mono that completes when all locks are renewed
     */
    public Mono<Void> renewMessageLockBatch(List<ServiceBusReceivedMessage> messages) {
        return Flux.fromIterable(messages)
                .flatMap(this::renewMessageLock)
                .then()
                .doOnSuccess(v -> logger.debug("Batch lock renewal completed"))
                .doOnError(e -> logger.error("Failed to renew locks batch", e));
    }

    /**
     * Prepares a message for sending, applying blob offloading logic if needed.
     *
     * @param messageBody the message body
     * @param applicationProperties application properties to add
     * @return the prepared ServiceBusMessage
     */
    private ServiceBusMessage prepareMessage(String messageBody, Map<String, Object> applicationProperties) {
        // If payload support is disabled, send directly
        if (!config.isPayloadSupportEnabled()) {
            ServiceBusMessage message = new ServiceBusMessage(messageBody);
            for (Map.Entry<String, Object> entry : applicationProperties.entrySet()) {
                message.getApplicationProperties().put(entry.getKey(), entry.getValue());
            }
            return message;
        }

        int payloadSize = messageBody.getBytes(StandardCharsets.UTF_8).length;
        boolean shouldOffload = config.isAlwaysThroughBlob() ||
                payloadSize > config.getMessageSizeThreshold();

        ServiceBusMessage message;
        Map<String, Object> properties = new HashMap<>(applicationProperties);

        // Validate application properties
        Set<String> reservedNames = new HashSet<>(Arrays.asList(
                ExtendedClientConfiguration.RESERVED_ATTRIBUTE_NAME,
                ExtendedClientConfiguration.LEGACY_RESERVED_ATTRIBUTE_NAME,
                ExtendedClientConfiguration.BLOB_POINTER_MARKER,
                ExtendedClientConfiguration.EXTENDED_CLIENT_USER_AGENT
        ));
        ApplicationPropertyValidator.validate(properties, reservedNames, config.getMaxAllowedProperties());

        if (shouldOffload) {
            // Generate unique blob name
            String blobName = config.getBlobKeyPrefix() + UUID.randomUUID().toString();

            // Store payload in blob
            BlobPointer pointer = payloadStore.storePayload(blobName, messageBody);

            // Create message with blob pointer as body
            message = new ServiceBusMessage(pointer.toJson());

            // Add metadata properties
            String attributeName = config.getReservedAttributeName();
            properties.put(attributeName, payloadSize);
            properties.put(ExtendedClientConfiguration.BLOB_POINTER_MARKER, "true");
        } else {
            message = new ServiceBusMessage(messageBody);
        }

        // Add user-agent tracking
        properties.put(ExtendedClientConfiguration.EXTENDED_CLIENT_USER_AGENT,
                "AzureServiceBusExtendedClient/1.0.0-SNAPSHOT");

        // Set application properties
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            message.getApplicationProperties().put(entry.getKey(), entry.getValue());
        }

        return message;
    }

    /**
     * Processes a received Service Bus message and resolves blob payload if needed.
     *
     * @param message the received Service Bus message
     * @return an ExtendedServiceBusMessage with resolved payload
     */
    private ExtendedServiceBusMessage processReceivedMessage(ServiceBusReceivedMessage message) {
        Map<String, Object> appProperties = new HashMap<>(message.getApplicationProperties());

        // Check for blob pointer marker
        boolean isFromBlob = "true".equals(String.valueOf(appProperties.get(ExtendedClientConfiguration.BLOB_POINTER_MARKER)));

        String body = message.getBody().toString();
        BlobPointer blobPointer = null;

        if (isFromBlob) {
            logger.debug("Message contains blob pointer. Resolving payload...");
            try {
                blobPointer = BlobPointer.fromJson(body);
                body = payloadStore.getPayload(blobPointer);

                // Handle case where blob is not found and ignorePayloadNotFound is true
                if (body == null && config.isIgnorePayloadNotFound()) {
                    logger.warn("Blob payload not found for message {}, returning empty body", message.getMessageId());
                    body = "";
                }

                // Remove internal marker properties
                appProperties.remove(ExtendedClientConfiguration.BLOB_POINTER_MARKER);

                // Remove both modern and legacy reserved attribute names
                appProperties.remove(ExtendedClientConfiguration.RESERVED_ATTRIBUTE_NAME);
                appProperties.remove(ExtendedClientConfiguration.LEGACY_RESERVED_ATTRIBUTE_NAME);

                logger.debug("Payload resolved from blob: {}", blobPointer);
            } catch (Exception e) {
                logger.error("Failed to resolve blob payload", e);
                throw new RuntimeException("Failed to resolve blob payload", e);
            }
        }

        // Strip user-agent property
        appProperties.remove(ExtendedClientConfiguration.EXTENDED_CLIENT_USER_AGENT);

        return new ExtendedServiceBusMessage(
                message.getMessageId(),
                body,
                appProperties,
                isFromBlob,
                blobPointer
        );
    }

    /**
     * Closes all Service Bus clients.
     */
    @Override
    public void close() {
        try {
            if (senderClient != null) {
                senderClient.close();
                logger.info("Async sender client closed");
            }
            if (receiverClient != null) {
                receiverClient.close();
                logger.info("Async receiver client closed");
            }
        } catch (Exception e) {
            logger.error("Error closing async clients", e);
        }
    }
}
