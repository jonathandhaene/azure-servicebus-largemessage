/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.util;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.OffsetDateTime;

/**
 * Utility class for generating Shared Access Signature (SAS) tokens for Azure Blob Storage.
 * Allows receivers to download blob payloads without needing storage account credentials.
 */
public class SasTokenGenerator {
    private static final Logger logger = LoggerFactory.getLogger(SasTokenGenerator.class);

    /**
     * Generates a SAS URI for the specified blob with read permission.
     *
     * @param blobClient the blob client for which to generate the SAS URI
     * @param validFor   the duration for which the SAS token should be valid
     * @return the SAS URI as a string
     * @throws RuntimeException if SAS URI generation fails
     */
    public static String generateBlobSasUri(BlobClient blobClient, Duration validFor) {
        try {
            logger.debug("Generating SAS URI for blob: {} with validity duration: {}", 
                        blobClient.getBlobName(), validFor);
            
            // Set permissions for read access only
            BlobSasPermission permissions = new BlobSasPermission().setReadPermission(true);
            
            // Calculate expiry time
            OffsetDateTime expiryTime = OffsetDateTime.now().plus(validFor);
            
            // Create signature values
            BlobServiceSasSignatureValues signatureValues = new BlobServiceSasSignatureValues(expiryTime, permissions);
            
            // Generate the SAS token and construct the full URI
            String sasToken = blobClient.generateSas(signatureValues);
            String sasUri = blobClient.getBlobUrl() + "?" + sasToken;
            
            logger.debug("Successfully generated SAS URI for blob: {}, expires at: {}", 
                        blobClient.getBlobName(), expiryTime);
            
            return sasUri;
        } catch (Exception e) {
            logger.error("Failed to generate SAS URI for blob: {}", blobClient.getBlobName(), e);
            throw new RuntimeException("Failed to generate SAS URI for blob", e);
        }
    }
}
