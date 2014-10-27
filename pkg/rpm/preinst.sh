#!/bin/sh
# Create bzzz user and group
USERNAME="bzzz"
GROUPNAME="bzzz"
getent group "$GROUPNAME" >/dev/null || groupadd -r "$GROUPNAME"
getent passwd "$USERNAME" >/dev/null || \
  useradd -r -g "$GROUPNAME" -d /usr/lib/bzzz -s /bin/false \
  -c "bzzz - lucene network wrapper" "$USERNAME"
exit 0
