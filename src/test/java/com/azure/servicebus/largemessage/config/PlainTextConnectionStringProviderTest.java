/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PlainTextConnectionStringProviderTest {

    @Test
    void testGetConnectionString() {
        // Arrange
        String expectedConnectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=abc123==;EndpointSuffix=core.windows.net";
        PlainTextConnectionStringProvider provider = new PlainTextConnectionStringProvider(expectedConnectionString);

        // Act
        String actualConnectionString = provider.getConnectionString();

        // Assert
        assertEquals(expectedConnectionString, actualConnectionString);
    }

    @Test
    void testGetConnectionString_EmptyString() {
        // Arrange
        String expectedConnectionString = "";
        PlainTextConnectionStringProvider provider = new PlainTextConnectionStringProvider(expectedConnectionString);

        // Act
        String actualConnectionString = provider.getConnectionString();

        // Assert
        assertEquals(expectedConnectionString, actualConnectionString);
    }

    @Test
    void testGetConnectionString_NullString() {
        // Arrange
        PlainTextConnectionStringProvider provider = new PlainTextConnectionStringProvider(null);

        // Act
        String actualConnectionString = provider.getConnectionString();

        // Assert
        assertNull(actualConnectionString);
    }

    @Test
    void testGetConnectionString_CalledMultipleTimes() {
        // Arrange
        String expectedConnectionString = "test-connection-string";
        PlainTextConnectionStringProvider provider = new PlainTextConnectionStringProvider(expectedConnectionString);

        // Act - call multiple times
        String result1 = provider.getConnectionString();
        String result2 = provider.getConnectionString();
        String result3 = provider.getConnectionString();

        // Assert - should return the same value every time
        assertEquals(expectedConnectionString, result1);
        assertEquals(expectedConnectionString, result2);
        assertEquals(expectedConnectionString, result3);
        assertEquals(result1, result2);
        assertEquals(result2, result3);
    }
}
