/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.store;

import com.azure.servicebus.largemessage.model.BlobPointer;

/**
 * Functional interface for customizing message body replacement after blob offloading.
 * Allows users to control what the message body becomes after the payload is stored in blob storage.
 */
@FunctionalInterface
public interface MessageBodyReplacer {
    /**
     * Replaces the original message body with a custom value after blob offloading.
     *
     * @param originalBody the original message body before offloading
     * @param pointer      the blob pointer referencing the stored payload
     * @return the replacement message body
     */
    String replace(String originalBody, BlobPointer pointer);
}
