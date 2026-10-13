/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package preflight

import (
	"strconv"
	"strings"
)

// MinFreeDiskMB matches the platform upgrade readiness floor. It is a minimum
// safety check, not an estimate of all image and workload storage requirements.
const MinFreeDiskMB = 256

// TimeSyncCommand checks the running provider's synchronization state without
// changing the system clock. Chrony's correction limit matches TIME-LAG's 5s.
const TimeSyncCommand = `
for service in chrony chronyd ntp ntpd systemd-timesyncd; do
  if systemctl is-active --quiet "$service"; then
    if case "$service" in
      chrony|chronyd) timeout 3 chronyc waitsync 1 5 0 0.1 ;;
      *) test "$(timeout 3 timedatectl show -p NTPSynchronized --value)" = yes ;;
    esac; then
      exit 0
    else
      echo "time synchronization service $service is running but synchronization is not confirmed (maximum correction 5s for chrony)" >&2
      exit 11
    fi
  fi
done
echo "no supported time synchronization service is running (chrony, ntp, or systemd-timesyncd)" >&2
exit 10
`

// DiskCommand checks the filesystem that will contain each configured path,
// including paths not yet created. It must run before installing packages.
func DiskCommand(paths ...string) string {
	quoted := make([]string, len(paths))
	for i, path := range paths {
		quoted[i] = "'" + strings.ReplaceAll(path, "'", "'\\''") + "'"
	}
	return `
for target in ` + strings.Join(quoted, " ") + `; do
  case "$target" in /*) ;; *) echo "disk precheck requires an absolute path: $target" >&2; exit 12 ;; esac
  path="$target"
  while [ ! -e "$path" ]; do path=$(dirname -- "$path"); done
  available=$(LC_ALL=C df -Pm -- "$path" | awk 'NR==2 {print $4}')
  case "$available" in ''|*[!0-9]*) echo "cannot read free disk space for $target" >&2; exit 12 ;; esac
  if [ "$available" -lt ` + strconv.Itoa(MinFreeDiskMB) + ` ]; then
    echo "insufficient disk space for $target: ${available}MB available, at least ` + strconv.Itoa(MinFreeDiskMB) + `MB required" >&2
    exit 12
  fi
done
`
}
