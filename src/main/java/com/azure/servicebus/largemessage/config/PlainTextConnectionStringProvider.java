package com.azure.servicebus.largemessage.config;

/**
 * Plain text implementation of StorageConnectionStringProvider.
 * Wraps a static connection string (current behavior).
 */
public class PlainTextConnectionStringProvider implements StorageConnectionStringProvider {
    private final String connectionString;

    /**
     * Creates a new PlainTextConnectionStringProvider with the specified connection string.
     *
     * @param connectionString the storage connection string
     */
    public PlainTextConnectionStringProvider(String connectionString) {
        this.connectionString = connectionString;
    }

    /**
     * Gets the storage connection string.
     *
     * @return the storage connection string
     */
    @Override
    public String getConnectionString() {
        return connectionString;
    }
}
