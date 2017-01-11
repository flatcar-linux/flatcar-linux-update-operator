// Package constants has Kubernetes label and annotation constants shared by
// the update-agent and update-operator.
package constants

const (
	// Annotation values used by update-agent and update-operator
	True  = "true"
	False = "false"

	// Prefix used by all label and annotation keys.
	Prefix = "container-linux-update.v1.coreos.com/"

	// Key set to "true" by the update-agent when a reboot is requested.
	AnnotationRebootNeeded = Prefix + "reboot-needed"

	// Key set to "true" by the update-agent when node-drain and reboot is
	// initiated.
	AnnotationRebootInProgress = Prefix + "reboot-in-progress"

	// Key set to "true" by the update-operator when an agent may proceed
	// with a node-drain and reboot.
	AnnotationOkToReboot = Prefix + "reboot-ok"

	// Key that may be set by the administrator to "true" to prevent
	// update-operator from considering a node for rebooting.  Never set by
	// the update-agent or update-operator.
	AnnotationRebootPaused = Prefix + "reboot-paused"

	// Key set by the update-agent to the current operator status of update_agent.
	//
	// Possible values are:
	//  - "UPDATE_STATUS_IDLE"
	//  - "UPDATE_STATUS_CHECKING_FOR_UPDATE"
	//  - "UPDATE_STATUS_UPDATE_AVAILABLE"
	//  - "UPDATE_STATUS_DOWNLOADING"
	//  - "UPDATE_STATUS_VERIFYING"
	//  - "UPDATE_STATUS_FINALIZING"
	//  - "UPDATE_STATUS_UPDATED_NEED_REBOOT"
	//  - "UPDATE_STATUS_REPORTING_ERROR_EVENT"
	//
	// It is possible, but extremely unlike for it to be "unknown status".
	AnnotationStatus = Prefix + "status"

	// Key set by the update-agent to the value of "ID" in /etc/os-release.
	LabelID = Prefix + "id"

	// Key set by the update-agent to the value of "GROUP" in
	// /usr/share/coreos/update.conf, overridden by the value of "GROUP" in
	// /etc/coreos/update.conf.
	LabelGroup = Prefix + "group"

	// Key set by the update-agent to the value of "VERSION" in /etc/os-release.
	LabelVersion = Prefix + "version"
)
