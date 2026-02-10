package com.azure.servicebus.extended.client;

import com.azure.core.util.BinaryData;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.servicebus.extended.config.ExtendedClientConfiguration;
import com.azure.servicebus.extended.model.BlobPointer;
import com.azure.servicebus.extended.model.ExtendedServiceBusMessage;
import com.azure.servicebus.extended.store.BlobPayloadStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AzureServiceBusExtendedClient.
 */
class AzureServiceBusExtendedClientTest {

    private ServiceBusSenderClient mockSenderClient;
    private ServiceBusReceiverClient mockReceiverClient;
    private BlobPayloadStore mockPayloadStore;
    private ExtendedClientConfiguration config;
    private AzureServiceBusExtendedClient client;

    @BeforeEach
    void setUp() {
        mockSenderClient = mock(ServiceBusSenderClient.class);
        mockReceiverClient = mock(ServiceBusReceiverClient.class);
        mockPayloadStore = mock(BlobPayloadStore.class);
        
        config = new ExtendedClientConfiguration();
        config.setMessageSizeThreshold(1024); // 1 KB for testing
        config.setAlwaysThroughBlob(false);
        config.setCleanupBlobOnDelete(true);
        config.setBlobKeyPrefix("");

        client = new AzureServiceBusExtendedClient(
                mockSenderClient,
                mockReceiverClient,
                mockPayloadStore,
                config
        );
    }

    @Test
    void testSmallMessage_sentDirectly() {
        // Arrange
        String smallMessage = "Small test message";
        ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        // Act
        client.sendMessage(smallMessage);

        // Assert
        verify(mockSenderClient, times(1)).sendMessage(messageCaptor.capture());
        ServiceBusMessage capturedMessage = messageCaptor.getValue();
        
        assertEquals(smallMessage, capturedMessage.getBody().toString());
        assertFalse(capturedMessage.getApplicationProperties().containsKey(
                ExtendedClientConfiguration.BLOB_POINTER_MARKER));
        
        // Verify no blob interaction
        verify(mockPayloadStore, never()).storePayload(anyString(), anyString());
    }

    @Test
    void testLargeMessage_offloadedToBlob() {
        // Arrange
        String largeMessage = generateMessage(2048); // 2 KB, exceeds threshold
        BlobPointer expectedPointer = new BlobPointer("test-container", "test-blob");
        
        when(mockPayloadStore.storePayload(anyString(), eq(largeMessage)))
                .thenReturn(expectedPointer);

        ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        // Act
        client.sendMessage(largeMessage);

        // Assert
        verify(mockPayloadStore, times(1)).storePayload(anyString(), eq(largeMessage));
        verify(mockSenderClient, times(1)).sendMessage(messageCaptor.capture());
        
        ServiceBusMessage capturedMessage = messageCaptor.getValue();
        String messageBody = capturedMessage.getBody().toString();
        
        // Verify message body contains blob pointer JSON
        assertTrue(messageBody.contains("containerName"));
        assertTrue(messageBody.contains("blobName"));
        
        // Verify application properties
        assertEquals("true", capturedMessage.getApplicationProperties().get(
                ExtendedClientConfiguration.BLOB_POINTER_MARKER));
        assertTrue(capturedMessage.getApplicationProperties().containsKey(
                ExtendedClientConfiguration.RESERVED_ATTRIBUTE_NAME));
    }

    @Test
    void testAlwaysThroughBlob_smallMessageStillOffloaded() {
        // Arrange
        config.setAlwaysThroughBlob(true);
        String smallMessage = "Small message";
        BlobPointer expectedPointer = new BlobPointer("test-container", "test-blob");
        
        when(mockPayloadStore.storePayload(anyString(), eq(smallMessage)))
                .thenReturn(expectedPointer);

        ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        // Act
        client.sendMessage(smallMessage);

        // Assert
        verify(mockPayloadStore, times(1)).storePayload(anyString(), eq(smallMessage));
        verify(mockSenderClient, times(1)).sendMessage(messageCaptor.capture());
        
        ServiceBusMessage capturedMessage = messageCaptor.getValue();
        assertEquals("true", capturedMessage.getApplicationProperties().get(
                ExtendedClientConfiguration.BLOB_POINTER_MARKER));
    }

    @Test
    void testDeletePayload_cleansUpBlob() {
        // Arrange
        BlobPointer pointer = new BlobPointer("test-container", "test-blob");
        ExtendedServiceBusMessage message = new ExtendedServiceBusMessage(
                "msg-id",
                "body",
                new HashMap<>(),
                true, // from blob
                pointer
        );

        // Act
        client.deletePayload(message);

        // Assert
        verify(mockPayloadStore, times(1)).deletePayload(pointer);
    }

    @Test
    void testDeletePayload_skipsWhenNotFromBlob() {
        // Arrange
        ExtendedServiceBusMessage message = new ExtendedServiceBusMessage(
                "msg-id",
                "body",
                new HashMap<>(),
                false, // not from blob
                null
        );

        // Act
        client.deletePayload(message);

        // Assert
        verify(mockPayloadStore, never()).deletePayload(any());
    }

    @Test
    void testDeletePayload_skipsWhenCleanupDisabled() {
        // Arrange
        config.setCleanupBlobOnDelete(false);
        BlobPointer pointer = new BlobPointer("test-container", "test-blob");
        ExtendedServiceBusMessage message = new ExtendedServiceBusMessage(
                "msg-id",
                "body",
                new HashMap<>(),
                true,
                pointer
        );

        // Act
        client.deletePayload(message);

        // Assert
        verify(mockPayloadStore, never()).deletePayload(any());
    }

    @Test
    void testSendMessage_withApplicationProperties() {
        // Arrange
        String messageBody = "Test message";
        Map<String, Object> customProps = new HashMap<>();
        customProps.put("customKey", "customValue");
        customProps.put("timestamp", 12345L);

        ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        // Act
        client.sendMessage(messageBody, customProps);

        // Assert
        verify(mockSenderClient, times(1)).sendMessage(messageCaptor.capture());
        ServiceBusMessage capturedMessage = messageCaptor.getValue();
        
        assertEquals("customValue", capturedMessage.getApplicationProperties().get("customKey"));
        assertEquals(12345L, capturedMessage.getApplicationProperties().get("timestamp"));
    }

    @Test
    void testBlobPointer_serializationRoundTrip() {
        // Arrange
        BlobPointer original = new BlobPointer("my-container", "my-blob-123");

        // Act
        String json = original.toJson();
        BlobPointer deserialized = BlobPointer.fromJson(json);

        // Assert
        assertNotNull(json);
        assertTrue(json.contains("my-container"));
        assertTrue(json.contains("my-blob-123"));
        assertEquals(original, deserialized);
        assertEquals(original.getContainerName(), deserialized.getContainerName());
        assertEquals(original.getBlobName(), deserialized.getBlobName());
    }

    @Test
    void testBlobPointer_equality() {
        // Arrange
        BlobPointer pointer1 = new BlobPointer("container", "blob");
        BlobPointer pointer2 = new BlobPointer("container", "blob");
        BlobPointer pointer3 = new BlobPointer("container", "different-blob");

        // Assert
        assertEquals(pointer1, pointer2);
        assertNotEquals(pointer1, pointer3);
        assertEquals(pointer1.hashCode(), pointer2.hashCode());
    }

    @Test
    void testBlobPointer_toString() {
        // Arrange
        BlobPointer pointer = new BlobPointer("test-container", "test-blob");

        // Act
        String string = pointer.toString();

        // Assert
        assertTrue(string.contains("test-container"));
        assertTrue(string.contains("test-blob"));
    }

    /**
     * Helper method to generate a message of specified size.
     */
    private String generateMessage(int sizeInBytes) {
        StringBuilder sb = new StringBuilder(sizeInBytes);
        for (int i = 0; i < sizeInBytes; i++) {
            sb.append('a');
        }
        return sb.toString();
    }
}
