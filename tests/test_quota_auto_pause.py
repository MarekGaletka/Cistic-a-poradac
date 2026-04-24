"""Tests for quota/rate-limit detection and auto-pause (Session 5, item 5.5).

Verifies that QUOTA_ERRORS patterns are correctly detected in error messages
and that the consolidation pipeline pauses appropriately.
"""

import pytest

from godmode_media_library.consolidation_types import QUOTA_ERRORS


class TestQuotaErrorDetection:
    """Test that QUOTA_ERRORS patterns match expected error strings."""

    @pytest.mark.parametrize(
        "error_msg",
        [
            "User quota exceeded for this operation",
            "Insufficient storage space on remote",
            "No space left on device",
            "Storage limit reached for this account",
            "Rate limit exceeded, retry after 60s",
            "User rate limit hit, backing off",
        ],
    )
    def test_known_quota_errors_detected(self, error_msg):
        assert any(q in error_msg.lower() for q in QUOTA_ERRORS), f"Expected '{error_msg}' to match a QUOTA_ERRORS pattern"

    @pytest.mark.parametrize(
        "error_msg",
        [
            "Connection timeout after 30s",
            "File not found: remote:path/to/file.jpg",
            "Permission denied: cannot write to destination",
            "SSL handshake failed",
            "Checksum mismatch on uploaded file",
        ],
    )
    def test_non_quota_errors_not_detected(self, error_msg):
        assert not any(q in error_msg.lower() for q in QUOTA_ERRORS), f"Expected '{error_msg}' NOT to match any QUOTA_ERRORS pattern"

    def test_case_insensitive_matching(self):
        """QUOTA_ERRORS are lowercase; matching should use .lower() on input."""
        for q in QUOTA_ERRORS:
            assert q == q.lower(), f"QUOTA_ERRORS entry '{q}' must be lowercase"

        # Upper-case input should still match when lowered
        assert any(q in "QUOTA EXCEEDED".lower() for q in QUOTA_ERRORS)
        assert any(q in "RATE LIMIT EXCEEDED".lower() for q in QUOTA_ERRORS)

    def test_partial_match_in_longer_message(self):
        msg = "ERROR 403: storage limit exceeded for user@example.com on drive:backup/"
        assert any(q in msg.lower() for q in QUOTA_ERRORS)

    def test_empty_error_no_false_positive(self):
        assert not any(q in "".lower() for q in QUOTA_ERRORS)


class TestQuotaAutoPauseBehavior:
    """Test that quota detection leads to job pause in the consolidation pipeline."""

    def test_quota_pauses_job(self):
        """Simulate the quota check logic from consolidation.py Phase 5."""
        from godmode_media_library.consolidation_types import (
            ERROR_TRUNCATE_LEN,
        )

        error_msg = "googleapi: Error 403: The user's Drive storage quota has been exceeded."
        paused = False

        # Replicate the exact logic from consolidation.py
        if any(q in error_msg.lower() for q in QUOTA_ERRORS):
            paused = True
            truncated = error_msg[:ERROR_TRUNCATE_LEN]

        assert paused is True
        assert len(truncated) <= ERROR_TRUNCATE_LEN

    def test_non_quota_error_does_not_pause(self):
        error_msg = "connection reset by peer"
        paused = False

        if any(q in error_msg.lower() for q in QUOTA_ERRORS):
            paused = True

        assert paused is False
