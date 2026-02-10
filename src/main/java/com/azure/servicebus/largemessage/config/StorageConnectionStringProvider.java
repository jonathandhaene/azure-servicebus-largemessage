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
