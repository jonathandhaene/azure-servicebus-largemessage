/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.config;

import java.util.Map;

/**
 * Functional interface for customizing message size criteria.
 * Provides maximum flexibility in determining when to offload messages to blob storage.
 */
@FunctionalInterface
public interface MessageSizeCriteria {
    /**
     * Determines whether a message should be offloaded to blob storage.
     *
     * @param messageBody           the message body
     * @param applicationProperties the message application properties
     * @return true if the message should be offloaded, false otherwise
     */
    boolean shouldOffload(String messageBody, Map<String, Object> applicationProperties);
}
