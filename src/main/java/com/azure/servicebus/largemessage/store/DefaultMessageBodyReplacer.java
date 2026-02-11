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
 * Default implementation of MessageBodyReplacer that replaces the message body 
 * with a JSON representation of the BlobPointer.
 * This is the current behavior of the library.
 */
public class DefaultMessageBodyReplacer implements MessageBodyReplacer {
    
    /**
     * Replaces the original message body with the BlobPointer JSON.
     *
     * @param originalBody the original message body (not used in default implementation)
     * @param pointer      the blob pointer referencing the stored payload
     * @return the JSON representation of the blob pointer
     */
    @Override
    public String replace(String originalBody, BlobPointer pointer) {
        return pointer.toJson();
    }
}
