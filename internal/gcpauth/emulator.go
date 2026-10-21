package gcpauth

import "fmt"

// SpannerEmulatorHost is the one documented local emulator authority. The
// authentication bypass is deliberately narrower than "any loopback": a
// caller must opt into experimental bootstrap and the environment must carry
// this exact value.
const SpannerEmulatorHost = "localhost:9010"

// ValidateSpannerEmulatorHost determines whether a Spanner client may disable
// authentication for the configured environment value.
func ValidateSpannerEmulatorHost(host string, allow bool) (bool, error) {
	if host == "" {
		return false, nil
	}
	if !allow {
		return false, fmt.Errorf("SPANNER_EMULATOR_HOST is forbidden outside experimental bootstrap")
	}
	if host != SpannerEmulatorHost {
		return false, fmt.Errorf("SPANNER_EMULATOR_HOST must equal %q when the experimental bypass is enabled", SpannerEmulatorHost)
	}
	return true, nil
}
