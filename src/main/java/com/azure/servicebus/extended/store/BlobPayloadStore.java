package com.azure.servicebus.extended.store;

import com.azure.servicebus.extended.model.BlobPointer;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

/**
 * Handles storage and retrieval of message payloads in Azure Blob Storage.
 * This is the Azure equivalent of AWS S3BackedPayloadStore.
 */
public class BlobPayloadStore {
    private static final Logger logger = LoggerFactory.getLogger(BlobPayloadStore.class);

    private final BlobContainerClient containerClient;
    private final String containerName;

    /**
     * Creates a new BlobPayloadStore.
     *
     * @param blobServiceClient the Azure Blob Service client
     * @param containerName     the name of the container to use for storing payloads
     */
    public BlobPayloadStore(BlobServiceClient blobServiceClient, String containerName) {
        this.containerName = containerName;
        this.containerClient = blobServiceClient.getBlobContainerClient(containerName);
        
        // Ensure the container exists
        if (!containerClient.exists()) {
            logger.info("Container '{}' does not exist. Creating it...", containerName);
            containerClient.create();
            logger.info("Container '{}' created successfully", containerName);
        } else {
            logger.debug("Container '{}' already exists", containerName);
        }
    }

    /**
     * Stores a payload in blob storage.
     *
     * @param blobName the name to use for the blob
     * @param payload  the payload to store
     * @return a BlobPointer referencing the stored payload
     */
    public BlobPointer storePayload(String blobName, String payload) {
        try {
            logger.debug("Storing payload in blob: {}", blobName);
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            
            byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(payloadBytes);
            
            blobClient.upload(inputStream, payloadBytes.length, true);
            logger.debug("Successfully stored payload in blob: {}", blobName);
            
            return new BlobPointer(containerName, blobName);
        } catch (Exception e) {
            logger.error("Failed to store payload in blob: {}", blobName, e);
            throw new RuntimeException("Failed to store payload in blob storage", e);
        }
    }

    /**
     * Retrieves a payload from blob storage.
     *
     * @param pointer the blob pointer referencing the payload
     * @return the payload content as a string
     */
    public String getPayload(BlobPointer pointer) {
        try {
            logger.debug("Retrieving payload from blob: {}", pointer.getBlobName());
            BlobClient blobClient = containerClient.getBlobClient(pointer.getBlobName());
            
            byte[] content = blobClient.downloadContent().toBytes();
            String payload = new String(content, StandardCharsets.UTF_8);
            
            logger.debug("Successfully retrieved payload from blob: {}", pointer.getBlobName());
            return payload;
        } catch (Exception e) {
            logger.error("Failed to retrieve payload from blob: {}", pointer.getBlobName(), e);
            throw new RuntimeException("Failed to retrieve payload from blob storage", e);
        }
    }

    /**
     * Deletes a payload from blob storage.
     * Handles 404 errors gracefully (blob already deleted or doesn't exist).
     *
     * @param pointer the blob pointer referencing the payload to delete
     */
    public void deletePayload(BlobPointer pointer) {
        try {
            logger.debug("Deleting payload from blob: {}", pointer.getBlobName());
            BlobClient blobClient = containerClient.getBlobClient(pointer.getBlobName());
            blobClient.delete();
            logger.debug("Successfully deleted payload from blob: {}", pointer.getBlobName());
        } catch (BlobStorageException e) {
            if (e.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND) {
                logger.debug("Blob not found (already deleted): {}", pointer.getBlobName());
            } else {
                logger.error("Failed to delete payload from blob: {}", pointer.getBlobName(), e);
                throw new RuntimeException("Failed to delete payload from blob storage", e);
            }
        } catch (Exception e) {
            logger.error("Failed to delete payload from blob: {}", pointer.getBlobName(), e);
            throw new RuntimeException("Failed to delete payload from blob storage", e);
        }
    }
}
