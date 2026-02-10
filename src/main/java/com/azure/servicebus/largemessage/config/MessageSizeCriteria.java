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
