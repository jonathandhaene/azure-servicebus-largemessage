package com.azure.servicebus.largemessage.util;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class SasTokenGeneratorTest {

    @Test
    void testGenerateBlobSasUri_Success() {
        // Arrange
        BlobClient blobClient = mock(BlobClient.class);
        when(blobClient.getBlobName()).thenReturn("test-blob");
        when(blobClient.getBlobUrl()).thenReturn("https://storage.blob.core.windows.net/container/test-blob");
        when(blobClient.generateSas(any(BlobServiceSasSignatureValues.class)))
            .thenReturn("sv=2023-01-01&sr=b&sig=abc123");

        Duration validFor = Duration.ofDays(7);

        // Act
        String sasUri = SasTokenGenerator.generateBlobSasUri(blobClient, validFor);

        // Assert
        assertNotNull(sasUri);
        assertTrue(sasUri.startsWith("https://storage.blob.core.windows.net/container/test-blob?"));
        assertTrue(sasUri.contains("sv=2023-01-01"));
        assertTrue(sasUri.contains("sig=abc123"));
        
        // Verify that generateSas was called with correct parameters
        ArgumentCaptor<BlobServiceSasSignatureValues> captor = ArgumentCaptor.forClass(BlobServiceSasSignatureValues.class);
        verify(blobClient).generateSas(captor.capture());
        
        BlobServiceSasSignatureValues capturedValue = captor.getValue();
        assertNotNull(capturedValue);
    }

    @Test
    void testGenerateBlobSasUri_NullBlobClient() {
        // Act & Assert
        assertThrows(NullPointerException.class, () -> 
            SasTokenGenerator.generateBlobSasUri(null, Duration.ofDays(1))
        );
    }

    @Test
    void testGenerateBlobSasUri_Exception() {
        // Arrange
        BlobClient blobClient = mock(BlobClient.class);
        when(blobClient.getBlobName()).thenReturn("test-blob");
        when(blobClient.generateSas(any(BlobServiceSasSignatureValues.class)))
            .thenThrow(new RuntimeException("SAS generation failed"));

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () -> 
            SasTokenGenerator.generateBlobSasUri(blobClient, Duration.ofDays(1))
        );
        
        assertTrue(exception.getMessage().contains("Failed to generate SAS URI"));
    }

    @Test
    void testGenerateBlobSasUri_CustomDuration() {
        // Arrange
        BlobClient blobClient = mock(BlobClient.class);
        when(blobClient.getBlobName()).thenReturn("test-blob");
        when(blobClient.getBlobUrl()).thenReturn("https://storage.blob.core.windows.net/container/test-blob");
        when(blobClient.generateSas(any(BlobServiceSasSignatureValues.class)))
            .thenReturn("sv=2023-01-01&sr=b&sig=xyz789");

        Duration validFor = Duration.ofHours(2);

        // Act
        String sasUri = SasTokenGenerator.generateBlobSasUri(blobClient, validFor);

        // Assert
        assertNotNull(sasUri);
        assertTrue(sasUri.contains("https://storage.blob.core.windows.net/container/test-blob?"));
        verify(blobClient).generateSas(any(BlobServiceSasSignatureValues.class));
    }
}
