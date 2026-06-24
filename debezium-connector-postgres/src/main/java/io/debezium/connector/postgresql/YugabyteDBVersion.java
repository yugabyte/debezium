/*
 * Copyright YugabyteDB Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A YugabyteDB server version, parsed from the {@code version()} string (e.g.
 * {@code "PostgreSQL 15.12-YB-2.31.0.0-b0 ..."}). Two release formats exist: stable/year-based
 * (e.g. {@code 2025.2.3.0}) and preview (e.g. {@code 2.31.0.0}); any trailing build suffix is ignored.
 * The two formats are never compared against each other — feature gates compare against the threshold
 * matching the detected format (see {@link #pkInRelationMessage()}).
 *
 * @author Shishir Sharma (ssharma@yugabyte.com)
 */
public class YugabyteDBVersion implements Comparable<YugabyteDBVersion> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBVersion.class);

    private static final Pattern YB_TOKEN_PATTERN = Pattern.compile("YB-([^\\s]+)");

    /** Number of numeric components parsed and compared (major/year, minor, patch, revision). */
    private static final int COMPONENTS = 4;

    /** Major component at/above this is the year-based line (2024.x, 2025.x); below it is preview (2.x). */
    private static final int YEAR_FORMAT_MAJOR_BOUNDARY = 2000;

    /** Sentinel used when the version cannot be determined or parsed. */
    public static final YugabyteDBVersion UNKNOWN = new YugabyteDBVersion("unknown", null);

    /**
     * First stable / year-based release that marks the primary key in the RELATION message for
     * {@code CHANGE} replica identity. At/above it the connector reads the PK from the message; below
     * it the message omits the PK, so the connector resolves it with a DB query.
     */
    private static final YugabyteDBVersion PK_IN_RELATION_MESSAGE_RI_CHANGE_STABLE = parse("2025.2.3.0");

    /** Preview equivalent of {@link #PK_IN_RELATION_MESSAGE_RI_CHANGE_STABLE}. */
    private static final YugabyteDBVersion PK_IN_RELATION_MESSAGE_RI_CHANGE_PREVIEW = parse("2.31.0.0");

    private final String raw;
    /** Numeric components padded to {@link #COMPONENTS}; {@code null} when unknown. */
    private final int[] components;

    private YugabyteDBVersion(String raw, int[] components) {
        this.raw = raw;
        this.components = components;
    }

    /** Extracts and parses the YB version from a full {@code version()} string; {@link #UNKNOWN} if absent. */
    public static YugabyteDBVersion fromVersionString(String fullVersionString) {
        if (fullVersionString == null) {
            return UNKNOWN;
        }
        final Matcher matcher = YB_TOKEN_PATTERN.matcher(fullVersionString);
        if (!matcher.find()) {
            LOGGER.warn("Could not find a YugabyteDB version token in '{}'", fullVersionString);
            return UNKNOWN;
        }
        return parse(matcher.group(1));
    }

    /** Parses a bare version token (e.g. {@code "2.31.0.0-b0"}); trailing build suffix ignored, {@link #UNKNOWN} on failure. */
    public static YugabyteDBVersion parse(String versionToken) {
        if (versionToken == null || versionToken.trim().isEmpty()) {
            return UNKNOWN;
        }
        // Drop any build / pre-release suffix: "2.31.0.0-b0" -> "2.31.0.0".
        final String numericPart = versionToken.trim().split("-")[0];
        final String[] parts = numericPart.split("\\.");
        final int[] parsed = new int[COMPONENTS];
        try {
            for (int i = 0; i < COMPONENTS && i < parts.length; i++) {
                parsed[i] = Integer.parseInt(parts[i].trim());
            }
        }
        catch (NumberFormatException e) {
            LOGGER.warn("Could not parse YugabyteDB version token '{}'", versionToken);
            return UNKNOWN;
        }
        return new YugabyteDBVersion(versionToken, parsed);
    }

    /** @return {@code true} if parsed; {@code false} for {@link #UNKNOWN}. */
    public boolean isKnown() {
        return components != null;
    }

    /** @return {@code true} for the year-based line (e.g. 2025.x); {@code false} for preview (2.x) or unknown. */
    public boolean isYearBased() {
        return isKnown() && components[0] >= YEAR_FORMAT_MAJOR_BOUNDARY;
    }

    /**
     * Whether this version marks the primary key in the RELATION message for {@code CHANGE} replica
     * identity (stable {@code >= 2025.2.3.0}, preview {@code >= 2.31.0.0}). At/above it the connector
     * reads the PK from the message; below it (or when unknown) it resolves the PK with a DB query.
     */
    public boolean pkInRelationMessage() {
        if (!isKnown()) {
            return false;
        }
        return isYearBased()
                ? compareTo(PK_IN_RELATION_MESSAGE_RI_CHANGE_STABLE) >= 0
                : compareTo(PK_IN_RELATION_MESSAGE_RI_CHANGE_PREVIEW) >= 0;
    }

    /** Numeric, component-by-component; note a year-based version sorts above a preview one. */
    @Override
    public int compareTo(YugabyteDBVersion otherVersion) {
        // Unknown sorts lowest.
        if (!isKnown() || !otherVersion.isKnown()) {
            return Boolean.compare(isKnown(), otherVersion.isKnown());
        }
        for (int i = 0; i < COMPONENTS; i++) {
            final int cmp = Integer.compare(components[i], otherVersion.components[i]);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof YugabyteDBVersion)) {
            return false;
        }
        final YugabyteDBVersion otherVersion = (YugabyteDBVersion) o;
        return Arrays.equals(components, otherVersion.components);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(components);
    }

    @Override
    public String toString() {
        return raw;
    }
}
