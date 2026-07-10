package drivers

import "testing"

func TestMicroVMLibkrunOnStopTarget(t *testing.T) {
	d := &microvm{}

	tests := []struct {
		name        string
		exitCode    int
		hasExitCode bool
		wantTarget  string
	}{
		{name: "exit status zero", exitCode: 0, hasExitCode: true, wantTarget: "reboot"},
		{name: "exit status one", exitCode: 1, hasExitCode: true, wantTarget: "stop"},
		{name: "missing exit status", exitCode: 0, hasExitCode: false, wantTarget: "stop"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			gotTarget := d.libkrunOnStopTarget(test.exitCode, test.hasExitCode)
			if gotTarget != test.wantTarget {
				t.Fatalf("libkrunOnStopTarget(%d, %t) = %q, want %q", test.exitCode, test.hasExitCode, gotTarget, test.wantTarget)
			}
		})
	}
}
