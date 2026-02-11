/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.config;

/**
 * Configuration for server-side encryption of blob payloads.
 */
public class EncryptionConfiguration {
    
    private String encryptionScope;
    private String customerProvidedKey;

    /**
     * Gets the encryption scope for blob storage.
     *
     * @return the encryption scope
     */
    public String getEncryptionScope() {
        return encryptionScope;
    }

    public void setEncryptionScope(String encryptionScope) {
        this.encryptionScope = encryptionScope;
    }

    /**
     * Gets the customer-provided encryption key.
     *
     * @return the customer-provided key
     */
    public String getCustomerProvidedKey() {
        return customerProvidedKey;
    }

    public void setCustomerProvidedKey(String customerProvidedKey) {
        this.customerProvidedKey = customerProvidedKey;
    }
}
