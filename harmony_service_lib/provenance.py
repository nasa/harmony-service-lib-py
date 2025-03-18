"""Utility functionality for generating provenance metadata."""

from datetime import datetime, UTC


def get_updated_history_metadata(
    service_name: str,
    service_version: str,
    existing_history: str | None = None,
) -> str:
    """Create updated the history global attribute.

    This function primarily ensures the correct formatting of the history
    string, and is agnostic to the format of the input file.

    """
    new_history_line = ' '.join(
        [
            datetime.now(UTC).isoformat(),
            service_name,
            service_version,
        ]
    )

    return '\n'.join(
        filter(None, [existing_history, new_history_line])
    )
