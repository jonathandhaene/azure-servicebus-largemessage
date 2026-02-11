/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.store;

import com.azure.messaging.servicebus.ServiceBusMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DefaultBlobNameResolverTest {

    @Test
    void testResolve_WithPrefix() {
        // Arrange
        String prefix = "test-prefix-";
        DefaultBlobNameResolver resolver = new DefaultBlobNameResolver(prefix);
        ServiceBusMessage message = new ServiceBusMessage("test body");

        // Act
        String blobName = resolver.resolve(message);

        // Assert
        assertNotNull(blobName);
        assertTrue(blobName.startsWith(prefix));
        assertTrue(blobName.length() > prefix.length());
    }

    @Test
    void testResolve_WithEmptyPrefix() {
        // Arrange
        String prefix = "";
        DefaultBlobNameResolver resolver = new DefaultBlobNameResolver(prefix);
        ServiceBusMessage message = new ServiceBusMessage("test body");

        // Act
        String blobName = resolver.resolve(message);

        // Assert
        assertNotNull(blobName);
        assertFalse(blobName.isEmpty());
    }

    @Test
    void testResolve_WithNullPrefix() {
        // Arrange
        DefaultBlobNameResolver resolver = new DefaultBlobNameResolver(null);
        ServiceBusMessage message = new ServiceBusMessage("test body");

        // Act
        String blobName = resolver.resolve(message);

        // Assert
        assertNotNull(blobName);
        assertFalse(blobName.isEmpty());
    }

    @Test
    void testResolve_GeneratesUniqueName() {
        // Arrange
        DefaultBlobNameResolver resolver = new DefaultBlobNameResolver("test-");
        ServiceBusMessage message = new ServiceBusMessage("test body");

        // Act
        String blobName1 = resolver.resolve(message);
        String blobName2 = resolver.resolve(message);

        // Assert
        assertNotNull(blobName1);
        assertNotNull(blobName2);
        assertNotEquals(blobName1, blobName2, "Each call should generate a unique blob name");
    }
}
