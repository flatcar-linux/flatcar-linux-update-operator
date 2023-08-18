package login1_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	godbus "github.com/godbus/dbus/v5"

	"github.com/flatcar-linux/flatcar-linux-update-operator/pkg/dbus"
	"github.com/flatcar-linux/flatcar-linux-update-operator/pkg/login1"
)

func Test_Creating_new_client(t *testing.T) {
	t.Parallel()

	t.Run("connects_to_global_login1_path_and_interface", func(t *testing.T) {
		t.Parallel()

		objectConstructorCalled := false

		connectionWithContextCheck := &dbus.MockConnection{
			ObjectF: func(dest string, path godbus.ObjectPath) godbus.BusObject {
				objectConstructorCalled = true

				expectedDest := "org.freedesktop.login1"

				if dest != expectedDest {
					t.Fatalf("Expected D-Bus destination %q, got %q", expectedDest, dest)
				}

				expectedPath := godbus.ObjectPath("/org/freedesktop/login1")

				if path != expectedPath {
					t.Fatalf("Expected D-Bus path %q, got %q", expectedPath, path)
				}

				return nil
			},
		}

		if _, err := login1.New(connectionWithContextCheck); err != nil {
			t.Fatalf("Unexpected error creating client: %v", err)
		}

		if !objectConstructorCalled {
			t.Fatalf("Expected object constructor to be called")
		}
	})

	t.Run("returns_error_when_no_objector_is_given", func(t *testing.T) {
		t.Parallel()

		client, err := login1.New(nil)
		if err == nil {
			t.Fatalf("Expected error creating client with no connector")
		}

		if client != nil {
			t.Fatalf("Expected client to be nil when New returns error")
		}
	})
}

//nolint:funlen // Many subtests.
func Test_Rebooting(t *testing.T) {
	t.Parallel()

	t.Run("calls_login1_reboot_method_on_manager_interface", func(t *testing.T) {
		t.Parallel()

		rebootCalled := false

		connectionWithContextCheck := &dbus.MockConnection{
			ObjectF: func(string, godbus.ObjectPath) godbus.BusObject {
				return &dbus.MockObject{
					CallWithContextF: func(ctx context.Context, method string, flags godbus.Flags, args ...interface{}) *godbus.Call {
						rebootCalled = true

						expectedMethodName := "org.freedesktop.login1.Manager.Reboot"

						if method != expectedMethodName {
							t.Fatalf("Expected method %q being called, got %q", expectedMethodName, method)
						}

						return &godbus.Call{}
					},
				}
			},
		}

		client, err := login1.New(connectionWithContextCheck)
		if err != nil {
			t.Fatalf("Unexpected error creating client: %v", err)
		}

		if err := client.Reboot(context.Background()); err != nil {
			t.Fatalf("Unexpected error rebooting: %v", err)
		}

		if !rebootCalled {
			t.Fatalf("Expected reboot method call on given D-Bus connection")
		}
	})

	t.Run("use_given_context_for_D-Bus_call", func(t *testing.T) {
		t.Parallel()

		testKey := struct{}{}
		expectedValue := "bar"

		ctx := context.WithValue(context.Background(), testKey, expectedValue)

		connectionWithContextCheck := &dbus.MockConnection{
			ObjectF: func(string, godbus.ObjectPath) godbus.BusObject {
				return &dbus.MockObject{
					CallWithContextF: func(ctx context.Context, method string, flags godbus.Flags, args ...interface{}) *godbus.Call {
						if val := ctx.Value(testKey); val != expectedValue {
							t.Fatalf("Got unexpected context on call")
						}

						return &godbus.Call{}
					},
				}
			},
		}

		client, err := login1.New(connectionWithContextCheck)
		if err != nil {
			t.Fatalf("Unexpected error creating client: %v", err)
		}

		if err := client.Reboot(ctx); err != nil {
			t.Fatalf("Unexpected error rebooting: %v", err)
		}
	})

	t.Run("returns_error_when_D-Bus_call_fails", func(t *testing.T) {
		t.Parallel()

		expectedError := fmt.Errorf("reboot error")

		connectionWithFailingObjectCall := &dbus.MockConnection{
			ObjectF: func(string, godbus.ObjectPath) godbus.BusObject {
				return &dbus.MockObject{
					CallWithContextF: func(ctx context.Context, method string, flags godbus.Flags, args ...interface{}) *godbus.Call {
						return &godbus.Call{
							Err: expectedError,
						}
					},
				}
			},
		}

		client, err := login1.New(connectionWithFailingObjectCall)
		if err != nil {
			t.Fatalf("Unexpected error creating client: %v", err)
		}

		if err := client.Reboot(context.Background()); !errors.Is(err, expectedError) {
			t.Fatalf("Unexpected error rebooting: %v", err)
		}
	})
}
