package main

import (
    "net/http/httptest"
    "testing"
)

// Test that CrossOriginProtection behaves the same as the previous ad-hoc
// Sec-Fetch-Site based checks for the common cases: "cross-site" and
// "same-site" should be rejected for unsafe methods, while "same-origin"
// and missing Sec-Fetch-Site should be allowed. Also verify our internal
// bypass pattern permits requests on /internal/ even when Sec-Fetch-Site is
// cross-site.
func TestCrossOriginProtection_SecFetchSiteParity(t *testing.T) {
    cp := newCrossOriginProtection(nil)

    cases := []struct {
        name    string
        path    string
        header  string
        wantErr bool
    }{
        {"cross-site forbidden", "/1.0/foo", "cross-site", true},
        {"same-site forbidden", "/1.0/foo", "same-site", true},
        {"same-origin allowed", "/1.0/foo", "same-origin", false},
        {"missing allowed", "/1.0/foo", "", false},
        {"internal bypass allowed", "/internal/foo", "cross-site", false},
    }

    for _, tc := range cases {
        tc := tc
        t.Run(tc.name, func(t *testing.T) {
            req := httptest.NewRequest("POST", "https://example.local"+tc.path, nil)
            if tc.header != "" {
                req.Header.Set("Sec-Fetch-Site", tc.header)
            }

            err := cp.Check(req)
            if (err != nil) != tc.wantErr {
                t.Fatalf("cp.Check() error = %v, wantErr %v", err, tc.wantErr)
            }
        })
    }
}
