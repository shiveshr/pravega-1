/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.segment;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * Utility methods for StreamSegment Names.
 */
public final class StreamSegmentNameUtils {
    //region Members

    /**
     * This is appended to the end of the Segment/Transaction name to indicate it stores its State.
     */
    private static final String STATE_SUFFIX = "$state";

    /**
     * This is appended to the end of the Segment/Transaction name to indicate it stores its extended attributes.
     */
    private static final String ATTRIBUTE_SUFFIX = "$attributes";

    /**
     * This is appended to the end of the Segment/Transaction name to indicate it stores its Rolling Storage Header.
     */
    private static final String HEADER_SUFFIX = "$header";

    /**
     * This is appended to the end of the Segment/Transaction name to indicate it represents a SegmentChunk.
     */
    private static final String OFFSET_SUFFIX = "$offset.";

    /**
     * This is appended to the end of the Parent Segment Name, then we append a unique identifier.
     */
    private static final String TRANSACTION_DELIMITER = "#transaction.";

    /**
     * This is appended to the end of the Primary Segment Name, followed by secondary id.
     */
    private static final String SECONDARY_ID_DELIMITER = "#secondary.";

    /**
     * The Transaction unique identifier is made of two parts, each having a length of 16 bytes (64 bits in Hex).
     */
    private static final int TRANSACTION_PART_LENGTH = Long.BYTES * 8 / 4;

    /**
     * The length of the Transaction unique identifier, in bytes (it is made of two parts).
     */
    private static final int TRANSACTION_ID_LENGTH = 2 * TRANSACTION_PART_LENGTH;

    /**
     * Custom String format that converts a 64 bit integer into a hex number, with leading zeroes.
     */
    private static final String FULL_HEX_FORMAT = "%0" + TRANSACTION_PART_LENGTH + "x";

    //endregion

    /**
     * Returns the transaction name for a TransactionStreamSegment based on the name of the current Parent StreamSegment, and the transactionId.
     *
     * @param parentStreamSegmentName The name of the Parent StreamSegment for this transaction.
     * @param transactionId           The unique Id for the transaction.
     * @return The name of the Transaction StreamSegmentId.
     */
    public static String getTransactionNameFromId(String parentStreamSegmentName, UUID transactionId) {
        StringBuilder result = new StringBuilder();
        result.append(parentStreamSegmentName);
        result.append(TRANSACTION_DELIMITER);
        result.append(String.format(FULL_HEX_FORMAT, transactionId.getMostSignificantBits()));
        result.append(String.format(FULL_HEX_FORMAT, transactionId.getLeastSignificantBits()));
        return result.toString();
    }

    /**
     * Attempts to extract the name of the Parent StreamSegment for the given Transaction StreamSegment. This method returns a
     * valid value only if the Transaction StreamSegmentName was generated using the generateTransactionStreamSegmentName method.
     *
     * @param transactionName The name of the Transaction StreamSegment to extract the name of the Parent StreamSegment.
     * @return The name of the Parent StreamSegment, or null if not a valid StreamSegment.
     */
    public static String getParentStreamSegmentName(String transactionName) {
        // Check to see if the given name is a properly formatted Transaction.
        int endOfStreamNamePos = transactionName.lastIndexOf(TRANSACTION_DELIMITER);
        if (endOfStreamNamePos < 0 || endOfStreamNamePos + TRANSACTION_DELIMITER.length() + TRANSACTION_ID_LENGTH > transactionName.length()) {
            // Improperly formatted Transaction name.
            return null;
        }
        return transactionName.substring(0, endOfStreamNamePos);
    }

    /**
     * Checks if the given stream segment name is formatted for a Transaction Segment or regular segment.
     *
     * @param streamSegmentName The name of the StreamSegment to check for transaction delimiter.
     * @return true if stream segment name contains transaction delimiter, false otherwise.
     */
    public static boolean isTransactionSegment(String streamSegmentName) {
        // Check to see if the given name is a properly formatted Transaction.
        int endOfStreamNamePos = streamSegmentName.lastIndexOf(TRANSACTION_DELIMITER);
        if (endOfStreamNamePos < 0 || endOfStreamNamePos + TRANSACTION_DELIMITER.length() + TRANSACTION_ID_LENGTH > streamSegmentName.length()) {
            return false;
        }
        return true;
    }

    /**
     * Attempts to extract the primary part of stream segment name before the secondary delimiter. This method returns a
     * valid value only if the StreamSegmentName was generated using the getQualifiedStreamSegmentName or getScopedPrimaryName method.
     *
     * @param streamSegmentName The name of the StreamSegment to extract the name of the Primary StreamSegment name.
     * @return The primary part of StreamSegment.
     */
    public static String extractPrimaryStreamSegmentName(String streamSegmentName) {
        if (isTransactionSegment(streamSegmentName)) {
            return extractPrimaryStreamSegmentName(getParentStreamSegmentName(streamSegmentName));
        } else {
            int endOfStreamNamePos = streamSegmentName.lastIndexOf(SECONDARY_ID_DELIMITER);
            return streamSegmentName.substring(0, endOfStreamNamePos);
        }
    }

    /**
     * Gets the name of the meta-Segment mapped to the given Segment Name that is responsible with storing Segment State.
     *
     * @param segmentName The name of the Segment to get the State segment name for.
     * @return The result.
     */
    public static String getStateSegmentName(String segmentName) {
        Preconditions.checkArgument(!segmentName.endsWith(STATE_SUFFIX), "segmentName is already a state segment name");
        return segmentName + STATE_SUFFIX;
    }

    /**
     * Gets the name of the meta-Segment mapped to the given Segment Name that is responsible with storing extended attributes.
     *
     * @param segmentName The name of the Segment to get the Attribute segment name for.
     * @return The result.
     */
    public static String getAttributeSegmentName(String segmentName) {
        Preconditions.checkArgument(!segmentName.endsWith(ATTRIBUTE_SUFFIX), "segmentName is already an attribute segment name");
        return segmentName + ATTRIBUTE_SUFFIX;
    }

    /**
     * Gets the name of the meta-Segment mapped to the given Segment Name that is responsible with storing its Rollover
     * information.
     * Existence of this file should also indicate that a Segment with this file has a rollover policy in place.
     *
     * @param segmentName The name of the Segment to get the Header segment name for.
     * @return The result.
     */
    public static String getHeaderSegmentName(String segmentName) {
        Preconditions.checkArgument(!segmentName.endsWith(HEADER_SUFFIX), "segmentName is already a segment header name");
        return segmentName + HEADER_SUFFIX;
    }

    /**
     * Gets the name of the Segment name from its Header Segment Name.
     *
     * @param headerSegmentName The name of the Header Segment.
     * @return The Segment Name.
     */
    public static String getSegmentNameFromHeader(String headerSegmentName) {
        Preconditions.checkArgument(headerSegmentName.endsWith(HEADER_SUFFIX));
        return headerSegmentName.substring(0, headerSegmentName.length() - HEADER_SUFFIX.length());
    }

    /**
     * Gets the name of the SegmentChunk for the given Segment and Offset.
     *
     * @param segmentName The name of the Segment to get the SegmentChunk name for.
     * @param offset      The starting offset of the SegmentChunk.
     * @return The SegmentChunk name.
     */
    public static String getSegmentChunkName(String segmentName, long offset) {
        Preconditions.checkArgument(!segmentName.contains(OFFSET_SUFFIX), "segmentName is already a SegmentChunk name");
        return segmentName + OFFSET_SUFFIX + Long.toString(offset);
    }

    /**
     * Method to compute 64 bit segment id which takes primary id and secondary id and composes it as
     * `msb = secondary` `lsb = primary`.
     * Primary id identifies the segment container mappeing and primary + secondary uniquely identifies a segment
     * within a stream.
     *
     * @param primaryId primary part of id.
     * @param secondaryId secondary part of id.
     * @return segment id which is composed using primary and secondary ids.
     */
    public static long computeSegmentId(int primaryId, int secondaryId) {
        return (long) secondaryId << 32 | (primaryId & 0xFFFFFFFFL);
    }

    /**
     * Method to extract primary id from given segment id.
     *
     * @param segmentId segment id.
     * @return primary part of segment id.
     */
    public static int getPrimaryId(long segmentId) {
        return (int) segmentId;
    }

    /**
     * Method to extract secondary id from given segment id.
     *
     * @param segmentId segment id.
     * @return secondary part of segment id.
     */
    public static int getSecondaryId(long segmentId) {
        return (int) (segmentId >> 32);
    }

    /**
     * Compose and return scoped stream name.
     *
     * @param scope scope to be used in ScopedStream name.
     * @param streamName stream name to be used in ScopedStream name.
     * @return scoped stream name.
     */
    public static String getScopedStreamName(String scope, String streamName) {
        return getScopedStreamNameInternal(scope, streamName).toString();
    }

    /**
     * Method to generate Fully Qualified StreamSegmentName using scope, stream and segment id.
     *
     * @param scope scope to be used in the ScopedStreamSegment name
     * @param streamName stream name to be used in ScopedStreamSegment name.
     * @param segmentId segment id to be used in ScopedStreamSegment name.
     * @return fully qualified StreamSegmentName.
     */
    public static String getQualifiedStreamSegmentName(String scope, String streamName, long segmentId) {
        int primaryId = getPrimaryId(segmentId);
        int secondaryId = getSecondaryId(segmentId);
        StringBuffer sb = getScopedStreamNameInternal(scope, streamName);
        sb.append('/');
        sb.append(primaryId);
        sb.append(SECONDARY_ID_DELIMITER);
        sb.append(secondaryId);
        return sb.toString();
    }

    /**
     * Method to extract different parts of stream segment name like scope, stream name and segment id from given
     * fully qualified segment name.
     * This function works even when scope is not set.
     *
     * @param qualifiedName StreamSegment's qualified name.
     * @return tokens capturing different components of stream segment name. Note: segmentId is extracted and sent back
     * as a String
     */
    public static List<String> extractSegmentTokens(String qualifiedName) {
        List<String> retVal = new LinkedList<>();
        String[] tokens = qualifiedName.split("[/]");
        int segmentIdIndex = tokens.length == 2 ? 1 : 2;
        long segmentId;
        if (tokens[segmentIdIndex].contains(SECONDARY_ID_DELIMITER)) {
            String[] segmentIdTokens = tokens[segmentIdIndex].split(SECONDARY_ID_DELIMITER);
            segmentId = computeSegmentId(Integer.parseInt(segmentIdTokens[0]), Integer.parseInt(segmentIdTokens[1]));
        } else {
            segmentId = computeSegmentId(Integer.parseInt(tokens[segmentIdIndex]), 0);
        }
        retVal.add(tokens[0]);
        if (tokens.length == 3) {
            retVal.add(tokens[1]);
        }
        retVal.add("" + segmentId);
        return retVal;
    }

    private static StringBuffer getScopedStreamNameInternal(String scope, String streamName) {
        StringBuffer sb = new StringBuffer();
        if (!Strings.isNullOrEmpty(scope)) {
            sb.append(scope);
            sb.append('/');
        }
        sb.append(streamName);
        return sb;
    }
}
