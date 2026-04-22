from app.repositories import group_repository


def _normalize_group_id(group_id):
    try:
        normalized_group_id = int(group_id)
    except (TypeError, ValueError):
        return None
    return normalized_group_id if normalized_group_id > 0 else None


def _normalize_version(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class GroupDeliveryGuard:
    def __init__(self, group_id, *, expected_membership_version=None):
        self.group_id = _normalize_group_id(group_id)
        self.expected_membership_version = _normalize_version(expected_membership_version)
        self.current_membership_version = 0
        self.snapshot_is_stale = False
        self._member_usernames = set()

        if self.group_id is None:
            return

        self.current_membership_version = group_repository.get_membership_version(self.group_id)
        self._member_usernames = set(
            group_repository.get_group_member_usernames(self.group_id)
        )
        if (
            self.expected_membership_version is not None
            and self.expected_membership_version != self.current_membership_version
        ):
            self.snapshot_is_stale = True

    def can_dispatch_to(self, username):
        normalized_username = (username or "").strip()
        if self.group_id is None or not normalized_username:
            return False

        self._refresh_if_version_changed()
        return normalized_username in self._member_usernames

    def _refresh_if_version_changed(self):
        if self.group_id is None:
            return

        latest_version = group_repository.get_membership_version(self.group_id)
        if latest_version == self.current_membership_version:
            return

        self.current_membership_version = latest_version
        self.snapshot_is_stale = True
        self._member_usernames = set(
            group_repository.get_group_member_usernames(self.group_id)
        )
