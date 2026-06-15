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
 * Represents a YugabyteDB server version and provides comparison helpers that can be used anywhere
 * in the connector to gate behaviour on the connected server version.
 *
 * <p>YugabyteDB advertises its version inside the standard Postgres {@code version()} string, for
 * example {@code "PostgreSQL 15.12-YB-2.31.0.0-b0 on ..."}. The YugabyteDB portion follows one of
 * two release formats:
 * <ul>
 *   <li><b>Stable / year-based</b> ({@code <year>.<minor>.<patch>.<revision>}) e.g. {@code 2024.1.0.0},
 *       {@code 2025.2.3.0}, {@code 2026.1.0.0}. Ordering: {@code 2024.1 < 2024.2 < 2025.1 < 2025.2}.</li>
 *   <li><b>Preview</b> ({@code <major>.<minor>.<patch>.<revision>}) e.g. {@code 2.27.0.0},
 *       {@code 2.29.0.0}, {@code 2.31.0.0}. Ordering: {@code 2.27 < 2.29 < 2.31}.</li>
 * </ul>
 * Any trailing build identifier such as {@code -b0} is ignored.
 *
 * <p>The two formats are never compared against each other for feature gating; instead a caller
 * compares against the threshold that matches the detected format (see
 * {@link #supportsChangeReplicaIdentityPkInRelation()}).
 *
 * @author Shishir Sharma (ssharma@yugabyte.com)
 */
public class YugabyteDBVersion implements Comparable<YugabyteDBVersion> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBVersion.class);

    /** Matches the YugabyteDB version token inside a Postgres {@code version()} string, e.g. {@code YB-2.31.0.0-b0}. */
    private static final Pattern YB_TOKEN_PATTERN = Pattern.compile("YB-([^\\s]+)");

    /** Number of numeric components that are parsed and compared (major/year, minor, patch, revision). */
    private static final int COMPONENTS = 4;

    /**
     * Major values at or above this boundary denote the stable / year-based release line
     * (e.g. {@code 2024.x}, {@code 2025.x}); anything below denotes the preview line
     * (e.g. {@code 2.27}, {@code 2.29}, {@code 2.31}).
     */
    private static final int YEAR_FORMAT_MAJOR_BOUNDARY = 2000;

    /** Sentinel used when the version cannot be determined or parsed. */
    public static final YugabyteDBVersion UNKNOWN = new YugabyteDBVersion("unknown", null);

    /**
     * First stable / year-based release that marks primary-key columns in the relation-message flags
     * for {@code CHANGE} replica identity (yugabyte-db commit {@code 5de43f9c6f8c}).
     */
    private static final YugabyteDBVersion CHANGE_PK_FIX_STABLE = parse("2025.2.3.0");

    /**
     * First preview release that marks primary-key columns in the relation-message flags for
     * {@code CHANGE} replica identity (yugabyte-db commit {@code 5de43f9c6f8c}).
     */
    private static final YugabyteDBVersion CHANGE_PK_FIX_PREVIEW = parse("2.29.0.0");

    private final String raw;
    /** Numeric version components padded to {@link #COMPONENTS}; {@code null} when the version is unknown. */
    private final int[] components;

    private YugabyteDBVersion(String raw, int[] components) {
        this.raw = raw;
        this.components = components;
    }

    /**
     * Extracts and parses the YugabyteDB version from a full Postgres {@code version()} string,
     * e.g. {@code "PostgreSQL 15.12-YB-2.31.0.0-b0 on ..."}.
     *
     * @param fullVersionString the value returned by {@code SELECT version()}; may be {@code null}
     * @return the parsed version, or {@link #UNKNOWN} if no YugabyteDB token is present
     */
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

    /**
     * Parses a bare YugabyteDB version token such as {@code "2.31.0.0-b0"} or {@code "2025.2.3.0"}.
     * This is the value produced by {@code SELECT substring(version() from 'YB-([^\\s]+)')}. Any
     * trailing build / pre-release identifier (e.g. {@code -b0}) is ignored.
     *
     * @param versionToken the version token; may be {@code null}
     * @return the parsed version, or {@link #UNKNOWN} if it cannot be parsed
     */
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

    /**
     * @return {@code true} if this version was successfully parsed, {@code false} for {@link #UNKNOWN}.
     */
    public boolean isKnown() {
        return components != null;
    }

    /**
     * @return {@code true} if this is a stable / year-based version (e.g. {@code 2024.x},
     *         {@code 2025.x}); {@code false} for the preview line (e.g. {@code 2.27}, {@code 2.29})
     *         or when the version is unknown.
     */
    public boolean isYearBased() {
        return isKnown() && components[0] >= YEAR_FORMAT_MAJOR_BOUNDARY;
    }

    /**
     * Indicates whether this YugabyteDB version marks primary-key columns in the relation-message
     * flags byte for {@code CHANGE} replica identity (yugabyte-db commit {@code 5de43f9c6f8c}).
     *
     * <p>When this returns {@code false} the pgoutput decoder must fall back to a DB query to
     * resolve PKs for {@code CHANGE} replica identity, because the flags do not carry the PK on
     * these older builds (YB#22555).
     *
     * <p>Thresholds: stable / year-based {@code >= 2025.2.3.0}; preview {@code >= 2.29.0.0}. An
     * unknown version is conservatively treated as <em>not</em> having the fix, so the safe DB
     * fallback is preserved.
     *
     * @return {@code true} if the flags reliably carry the PK for {@code CHANGE} replica identity.
     */
    public boolean supportsChangeReplicaIdentityPkInRelation() {
        if (!isKnown()) {
            return false;
        }
        return isYearBased()
                ? compareTo(CHANGE_PK_FIX_STABLE) >= 0
                : compareTo(CHANGE_PK_FIX_PREVIEW) >= 0;
    }

    /**
     * Compares two versions component by component. Note that this is a purely numeric comparison,
     * so a year-based version always sorts above a preview version (e.g. {@code 2025.x > 2.x}); for
     * feature gating compare against a threshold of the same format instead.
     */
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
