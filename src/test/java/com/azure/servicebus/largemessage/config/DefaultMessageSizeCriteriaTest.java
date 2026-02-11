/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DefaultMessageSizeCriteriaTest {

    @Test
    void testShouldOffload_ExceedsThreshold() {
        // Arrange
        int threshold = 100;
        DefaultMessageSizeCriteria criteria = new DefaultMessageSizeCriteria(threshold, false);
        String largeMessage = "a".repeat(200); // 200 bytes > 100 threshold
        Map<String, Object> properties = new HashMap<>();

        // Act
        boolean result = criteria.shouldOffload(largeMessage, properties);

        // Assert
        assertTrue(result, "Message exceeding threshold should be offloaded");
    }

    @Test
    void testShouldOffload_BelowThreshold() {
        // Arrange
        int threshold = 100;
        DefaultMessageSizeCriteria criteria = new DefaultMessageSizeCriteria(threshold, false);
        String smallMessage = "a".repeat(50); // 50 bytes < 100 threshold
        Map<String, Object> properties = new HashMap<>();

        // Act
        boolean result = criteria.shouldOffload(smallMessage, properties);

        // Assert
        assertFalse(result, "Message below threshold should not be offloaded");
    }

    @Test
    void testShouldOffload_AlwaysThroughBlob() {
        // Arrange
        int threshold = 1000;
        DefaultMessageSizeCriteria criteria = new DefaultMessageSizeCriteria(threshold, true);
        String smallMessage = "tiny"; // Much smaller than threshold
        Map<String, Object> properties = new HashMap<>();

        // Act
        boolean result = criteria.shouldOffload(smallMessage, properties);

        // Assert
        assertTrue(result, "When alwaysThroughBlob is true, all messages should be offloaded");
    }

    @Test
    void testShouldOffload_ExactlyAtThreshold() {
        // Arrange
        int threshold = 100;
        DefaultMessageSizeCriteria criteria = new DefaultMessageSizeCriteria(threshold, false);
        String exactMessage = "a".repeat(100); // Exactly 100 bytes
        Map<String, Object> properties = new HashMap<>();

        // Act
        boolean result = criteria.shouldOffload(exactMessage, properties);

        // Assert
        assertFalse(result, "Message exactly at threshold should not be offloaded");
    }

    @Test
    void testShouldOffload_EmptyMessage() {
        // Arrange
        int threshold = 100;
        DefaultMessageSizeCriteria criteria = new DefaultMessageSizeCriteria(threshold, false);
        String emptyMessage = "";
        Map<String, Object> properties = new HashMap<>();

        // Act
        boolean result = criteria.shouldOffload(emptyMessage, properties);

        // Assert
        assertFalse(result, "Empty message should not be offloaded");
    }

    @Test
    void testShouldOffload_IgnoresApplicationProperties() {
        // Arrange
        int threshold = 100;
        DefaultMessageSizeCriteria criteria = new DefaultMessageSizeCriteria(threshold, false);
        String message = "a".repeat(150);
        Map<String, Object> properties = new HashMap<>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");

        // Act
        boolean result = criteria.shouldOffload(message, properties);

        // Assert
        assertTrue(result, "Application properties should not affect default criteria");
    }
}
