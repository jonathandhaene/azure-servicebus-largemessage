/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.store;

import com.azure.servicebus.largemessage.model.BlobPointer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DefaultMessageBodyReplacerTest {

    @Test
    void testReplace_ReturnsPointerJson() {
        // Arrange
        DefaultMessageBodyReplacer replacer = new DefaultMessageBodyReplacer();
        String originalBody = "Original message body content";
        BlobPointer pointer = new BlobPointer("test-container", "test-blob-name");

        // Act
        String result = replacer.replace(originalBody, pointer);

        // Assert
        assertNotNull(result);
        assertTrue(result.contains("test-container"));
        assertTrue(result.contains("test-blob-name"));
        assertEquals(pointer.toJson(), result);
    }

    @Test
    void testReplace_IgnoresOriginalBody() {
        // Arrange
        DefaultMessageBodyReplacer replacer = new DefaultMessageBodyReplacer();
        String originalBody1 = "First original body";
        String originalBody2 = "Second original body";
        BlobPointer pointer = new BlobPointer("container", "blob");

        // Act
        String result1 = replacer.replace(originalBody1, pointer);
        String result2 = replacer.replace(originalBody2, pointer);

        // Assert - results should be the same since original body is ignored
        assertEquals(result1, result2);
        assertEquals(pointer.toJson(), result1);
        assertEquals(pointer.toJson(), result2);
    }

    @Test
    void testReplace_WithNullOriginalBody() {
        // Arrange
        DefaultMessageBodyReplacer replacer = new DefaultMessageBodyReplacer();
        BlobPointer pointer = new BlobPointer("container", "blob");

        // Act
        String result = replacer.replace(null, pointer);

        // Assert
        assertNotNull(result);
        assertEquals(pointer.toJson(), result);
    }

    @Test
    void testReplace_WithEmptyOriginalBody() {
        // Arrange
        DefaultMessageBodyReplacer replacer = new DefaultMessageBodyReplacer();
        BlobPointer pointer = new BlobPointer("container", "blob");

        // Act
        String result = replacer.replace("", pointer);

        // Assert
        assertNotNull(result);
        assertEquals(pointer.toJson(), result);
    }
}
