# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.10.0] - 2023-07-24
### Added
- Agent now supports `-force-drain` flag, which allows removal of pods with custom or no controller when draining nodes [#197](https://github.com/flatcar/flatcar-linux-update-operator/pull/197).

### Changed
- Everything is now built using Go 1.20.
- Container images now use Alpine Linux version v3.18 as a base image.

## [0.9.0] - 2023-01-03
### Added
- Example manifests use new `node-role.kubernetes.io/control-plane` tolerations in addition to legacy `node-role.kubernetes.io/master`.
- Added release pipeline which will be building release container images from now on.
- Added compatibility with Kubernetes versions v1.24+.
- `kustomization.yaml` file has been added to example manifests to enable using them with `kustomize`.

### Changed
- Moved from `github.com/flatcar-linux/flatcar-linux-update-operator` to `github.com/flatcar/flatcar-linux-update-operator`. This also means that the docker images will be now available at `ghcr.io/flatcar/flatcar-linux-update-operator`. The `0.8.0` image is still available at the old location, but no new images will be pushed there.
- Everything is now built using Go 1.19.
- Updated all Go dependencies.
- Dropped dependency on `github.com/flatcar-linux/locksmith`.
- In example manifests use Role resource instead of ClusterRole where possible to limit scope of permissions.
- Limited operator access to config maps only to one used for leader election in example manifests.
- Improved agent responsiveness to context cancellation.
- Agent code is now covered with tests and some initial refactorings has been performed.
- Container images now use Alpine Linux version v3.15 as a base image.
- Draining pods is now done using Eviction API when possible.
- Capitalization of log messages and formatting of produced errors is now more consistent.
- `latest` tag for container images will no longer be used. Users are recommended to use specific versions.

### Fixed
- Publishing leader election events by operator.
- Agent main loop leaking goroutine when agent is stopped.
- Used service account by agent in example manifests.

### Removed
- Unused RBAC permissions from agent and operator example manifest.
- Unreachable agent code.
- Deprecated `--auto-label-flatcar-linux` flag has been removed from operator.
- Pod Security Policies has been removed from example manifests, as their support has been removed in Kubernetes 1.25.

## [0.8.0] - 2021-09-24
### Added
- Added golangci-lint and codespell CI jobs using GitHub Actions.
- Added CI for vendor directory, generated files, Go modules tidiness, Dockerfile etc.
- Added missing documentation for all packages.
- Added integration tests for `pkg/updateengine`.
- Added unit tests for `pkg/operator`.

### Changed
- Updated used Go version to 1.16 and Alpine version for container images to 3.14.
- Container images will be now published to `ghcr.io/flatcar-linux/flatcar-linux-update-operator` instead of
`quay.io/kinvolk/flatcar-linux-update-operator`.
- `update-agent` now use a dedicated Service Account with lower privileges than the operator.
- `update-agent` now use a dedicated Pod Security Policy.
- `update-operator` now use more strict Pod Security Policy, as it no longer needs permissions
for agent related resources.
- `Flatcar Linux` references has been changed to `Flatcar Container Linux`.
- Migrated from [glog](https://github.com/golang/glog) to [klog](https://github.com/kubernetes/klog),
which is a recommended logger for Kubernetes operators.
- `pkg/drain` has been integrated into `pkg/k8sutil`.
- Errors returned from operator and agent runs are now properly wrapped and can be unwrapped by the user.
- Remaining CoreOS references has been replaced with Flatcar.
- `pkg/k8sutil.GetVersionInfo()` will now return an error when `/etc/flatcar/update.conf` file is present,
but not readable.
- Marking node as Unschedulable should now be more robust on frequent Node object updates.
- Renamed Go module from `github.com/kinvolk/flatcar-linux-update-operator` to `github.com/flatcar-linux/flatcar-linux-update-operator`, following the move of repository into new organization.

### Fixed
- Fixed various typos found by codespell.
- Operator will no longer leak leader election goroutine, it will be now shut down when stop channel gets closed.
- Operator will now internally gracefully return an error when leader election is lost, so process can be restarted
to start a new leader election.

### Removed
- Removed no longer used Travis CI, Jenkins configuration files and build scripts.
- Removed `AttemptUpdate()`, `GetStatus()` and `RebootNeededSignal()` functions from `pkg/updateengine` package.
- Removed deprecated `--manage-agent`, `--agent-image-repo` and `--analytics` flags from `update-operator`.
- Deprecated operator functionality to manage agent DaemonSet has been removed. Agent DaemonSet must be now deployed separately.

## [0.7.3] - 2020-01-14
### Changed
- Migrated to Go modules.
- Updated all Go dependencies to latest versions.
- Use vendored dependencies while building binaries.
- `update-agent` example manifests now enforce running agent as root, to ensure agent
is able to trigger a reboot on the host.

### Fixed
- Fixed example manifests for Kubernetes 1.17 compatibility.
- Made operator code compatible with Kubernetes 1.17.
- Fixed publishing leader acquired event by the operator.

[0.10.0]: https://github.com/flatcar/flatcar-linux-update-operator/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/flatcar/flatcar-linux-update-operator/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/flatcar/flatcar-linux-update-operator/compare/v0.7.3...v0.8.0
[0.7.3]: https://github.com/flatcar/flatcar-linux-update-operator/compare/v0.7.2...v0.7.3
