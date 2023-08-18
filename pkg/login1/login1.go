// Package login1 is a small subset of github.com/coreos/go-systemd/v22/login1 package with
// ability to use shared D-Bus connection and with proper error handling for Reboot method call, which
// is not yet provided by the upstream.
package login1

import (
	"context"
	"fmt"

	godbus "github.com/godbus/dbus/v5"
)

// Client describes functionality of provided login1 client.
type Client interface {
	Reboot(context.Context) error
}

// Objector describes functionality required from a given D-Bus connection.
type Objector interface {
	Object(string, godbus.ObjectPath) godbus.BusObject
}

// Caller describes required functionality from D-Bus object.
type Caller interface {
	CallWithContext(ctx context.Context, method string, flags godbus.Flags, args ...interface{}) *godbus.Call
}

// New creates new login1 client using given D-Bus connection.
func New(objector Objector) (Client, error) {
	if objector == nil {
		return nil, fmt.Errorf("no objector given")
	}

	// Object path used by systemd-logind.
	dbusDest := "org.freedesktop.login1"

	// Standard path to systemd-logind interface.
	dbusPath := godbus.ObjectPath("/org/freedesktop/login1")

	return &rebooter{
		caller: objector.Object(dbusDest, dbusPath),
	}, nil
}

// Reboot reboots machine on which it's called.
func (r *rebooter) Reboot(ctx context.Context) error {
	// Systemd-logind interface name.
	dbusInterface := "org.freedesktop.login1.Manager"

	// Login1 manager interface method name responsible for rebooting.
	dbusMethodNameReboot := "Reboot"

	if call := r.caller.CallWithContext(ctx, dbusInterface+"."+dbusMethodNameReboot, 0, false); call.Err != nil {
		return fmt.Errorf("calling reboot: %w", call.Err)
	}

	return nil
}

// Rebooter is an internal type implementing Client interface.
type rebooter struct {
	caller Caller
}

// Rebooter must implement Client interface.
var _ Client = &rebooter{}
