/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.config;

/**
 * Interface for providing storage connection strings dynamically.
 * Allows integration with dynamic credential sources like Azure Key Vault.
 */
public interface StorageConnectionStringProvider {
    /**
     * Gets the storage connection string.
     * This method is called when the connection string is needed.
     *
     * @return the storage connection string
     */
    String getConnectionString();
}
