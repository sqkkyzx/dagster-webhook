"""Request validation shared by the legacy and resource submission APIs."""

from dagster._core.storage.tags import REPOSITORY_LABEL_TAG

SUBMISSION_TAG = "dagster-webhook/submission_id"


def validate_submission_tags(tags):
    """Protect Job origin and the gateway-owned submission correlation marker."""
    for key in (REPOSITORY_LABEL_TAG, SUBMISSION_TAG):
        if tags is not None and key in tags:
            raise ValueError(f"Tag {key!r} is reserved for gateway-managed metadata")
    return tags
