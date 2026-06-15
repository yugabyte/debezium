/*
 * Copyright YugabyteDB Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

/**
 * Unit tests for {@link YugabyteDBVersion} parsing, ordering, and feature-gate thresholds.
 *
 * @author Shishir Sharma (ssharma@yugabyte.com)
 */
public class YugabyteDBVersionTest {

    @Test
    public void shouldParsePreviewTokenIgnoringBuildSuffix() {
        YugabyteDBVersion version = YugabyteDBVersion.parse("2.31.0.0-b0");
        assertThat(version.isKnown()).isTrue();
        assertThat(version.isYearBased()).isFalse();
        assertThat(version.toString()).isEqualTo("2.31.0.0-b0");
    }

    @Test
    public void shouldParseYearBasedToken() {
        YugabyteDBVersion version = YugabyteDBVersion.parse("2025.2.3.0");
        assertThat(version.isKnown()).isTrue();
        assertThat(version.isYearBased()).isTrue();
    }

    @Test
    public void shouldExtractVersionFromFullVersionString() {
        // The exact form returned by SELECT version() on YugabyteDB.
        String full = "PostgreSQL 15.12-YB-2.31.0.0-b0 on aarch64-apple-darwin24.6.0, compiled by clang";
        YugabyteDBVersion version = YugabyteDBVersion.fromVersionString(full);
        assertThat(version.isKnown()).isTrue();
        assertThat(version.isYearBased()).isFalse();
        assertThat(version).isEqualTo(YugabyteDBVersion.parse("2.31.0.0"));
    }

    @Test
    public void shouldReturnUnknownForUnparseableInput() {
        assertThat(YugabyteDBVersion.parse(null)).isEqualTo(YugabyteDBVersion.UNKNOWN);
        assertThat(YugabyteDBVersion.parse("")).isEqualTo(YugabyteDBVersion.UNKNOWN);
        assertThat(YugabyteDBVersion.parse("not-a-version")).isEqualTo(YugabyteDBVersion.UNKNOWN);
        assertThat(YugabyteDBVersion.fromVersionString("PostgreSQL 15.12 (no YB token)"))
                .isEqualTo(YugabyteDBVersion.UNKNOWN);
        assertThat(YugabyteDBVersion.UNKNOWN.isKnown()).isFalse();
    }

    @Test
    public void shouldOrderYearBasedVersions() {
        // 2024.1 < 2024.2 < 2025.1 < 2025.2
        assertThat(YugabyteDBVersion.parse("2024.1.0.0")).isLessThan(YugabyteDBVersion.parse("2024.2.0.0"));
        assertThat(YugabyteDBVersion.parse("2024.2.0.0")).isLessThan(YugabyteDBVersion.parse("2025.1.0.0"));
        assertThat(YugabyteDBVersion.parse("2025.1.0.0")).isLessThan(YugabyteDBVersion.parse("2025.2.0.0"));
    }

    @Test
    public void shouldOrderPreviewVersions() {
        // 2.27 < 2.29 < 2.31
        assertThat(YugabyteDBVersion.parse("2.27.0.0")).isLessThan(YugabyteDBVersion.parse("2.29.0.0"));
        assertThat(YugabyteDBVersion.parse("2.29.0.0")).isLessThan(YugabyteDBVersion.parse("2.31.0.0"));
    }

    @Test
    public void shouldTreatMissingComponentsAsZero() {
        // "2.29" should be equivalent to "2.29.0.0".
        assertThat(YugabyteDBVersion.parse("2.29")).isEqualByComparingTo(YugabyteDBVersion.parse("2.29.0.0"));
        assertThat(YugabyteDBVersion.parse("2025.2.3")).isEqualByComparingTo(YugabyteDBVersion.parse("2025.2.3.0"));
    }

    @Test
    public void previewVersionsAtOrAboveThresholdSupportChangePkFix() {
        // Preview threshold is 2.29.0.0.
        assertThat(YugabyteDBVersion.parse("2.29.0.0").supportsChangeReplicaIdentityPkInRelation()).isTrue();
        assertThat(YugabyteDBVersion.parse("2.29.1.0").supportsChangeReplicaIdentityPkInRelation()).isTrue();
        assertThat(YugabyteDBVersion.parse("2.31.0.0-b0").supportsChangeReplicaIdentityPkInRelation()).isTrue();
    }

    @Test
    public void previewVersionsBelowThresholdDoNotSupportChangePkFix() {
        assertThat(YugabyteDBVersion.parse("2.27.0.0").supportsChangeReplicaIdentityPkInRelation()).isFalse();
        assertThat(YugabyteDBVersion.parse("2.28.9.9").supportsChangeReplicaIdentityPkInRelation()).isFalse();
    }

    @Test
    public void yearBasedVersionsAtOrAboveThresholdSupportChangePkFix() {
        // Year-based threshold is 2025.2.3.0.
        assertThat(YugabyteDBVersion.parse("2025.2.3.0").supportsChangeReplicaIdentityPkInRelation()).isTrue();
        assertThat(YugabyteDBVersion.parse("2025.2.4.0").supportsChangeReplicaIdentityPkInRelation()).isTrue();
        assertThat(YugabyteDBVersion.parse("2026.1.0.0").supportsChangeReplicaIdentityPkInRelation()).isTrue();
    }

    @Test
    public void yearBasedVersionsBelowThresholdDoNotSupportChangePkFix() {
        assertThat(YugabyteDBVersion.parse("2025.2.2.0").supportsChangeReplicaIdentityPkInRelation()).isFalse();
        assertThat(YugabyteDBVersion.parse("2025.1.0.0").supportsChangeReplicaIdentityPkInRelation()).isFalse();
        assertThat(YugabyteDBVersion.parse("2024.2.0.0").supportsChangeReplicaIdentityPkInRelation()).isFalse();
    }

    @Test
    public void unknownVersionConservativelyDoesNotSupportChangePkFix() {
        // When the version cannot be determined we keep the safe DB fallback.
        assertThat(YugabyteDBVersion.UNKNOWN.supportsChangeReplicaIdentityPkInRelation()).isFalse();
    }
}
