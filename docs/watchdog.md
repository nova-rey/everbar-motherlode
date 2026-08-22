# Motherlode health watchdog

`scripts/motherlode-watchdog.sh` is a cheap, local hourly probe. It connects to
the persistent Lightning studio and checks the four dataset workers, wave
controller, progress monitor, current receipt freshness, and conversion-count
movement. It never alters corpus data or restarts a worker itself.

On a fault it uses the local Codex CLI's `queue` operation to enqueue a clear
repair request into the configured Codex thread. This is intentionally an
escalation—not an autonomous data-policy decision—and alerts are debounced for
four hours unless the fault changes.

The target thread, SSH target, corpus root, and local state directory live in
an external mode-0600 configuration file, not Git. The provided user systemd
timer runs on boot and then every hour; `Persistent=true` makes a missed timer
run promptly after the local host returns.

Useful inspection commands:

```bash
systemctl --user status everbar-motherlode-watchdog.timer
systemctl --user start everbar-motherlode-watchdog.service
journalctl --user -u everbar-motherlode-watchdog.service -n 50 --no-pager
```
