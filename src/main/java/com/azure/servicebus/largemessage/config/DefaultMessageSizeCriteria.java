/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.config;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Default implementation of MessageSizeCriteria.
 * Uses a fixed byte threshold to determine when to offload messages (current behavior).
 */
public class DefaultMessageSizeCriteria implements MessageSizeCriteria {
    private final int messageSizeThreshold;
    private final boolean alwaysThroughBlob;

    /**
     * Creates a new DefaultMessageSizeCriteria with the specified configuration.
     *
     * @param messageSizeThreshold the message size threshold in bytes
     * @param alwaysThroughBlob    whether all messages should go through blob storage
     */
    public DefaultMessageSizeCriteria(int messageSizeThreshold, boolean alwaysThroughBlob) {
        this.messageSizeThreshold = messageSizeThreshold;
        this.alwaysThroughBlob = alwaysThroughBlob;
    }

    /**
     * Determines whether a message should be offloaded based on size threshold.
     *
     * @param messageBody           the message body
     * @param applicationProperties the message application properties (not used in default implementation)
     * @return true if message size exceeds threshold or alwaysThroughBlob is enabled
     */
    @Override
    public boolean shouldOffload(String messageBody, Map<String, Object> applicationProperties) {
        if (alwaysThroughBlob) {
            return true;
        }
        int payloadSize = messageBody.getBytes(StandardCharsets.UTF_8).length;
        return payloadSize > messageSizeThreshold;
    }
}
