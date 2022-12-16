package zk

import "testing"

func TestParseVersion(t *testing.T) {
	t.Parallel()
	tt := []struct {
		vs          string
		wantVersion Version
	}{
		{vs: "1.2.3", wantVersion: Version{1, 2, 3}},
		{vs: "0.0.0", wantVersion: Version{0, 0, 0}},
		{vs: "1.0.0", wantVersion: Version{1, 0, 0}},
		{vs: "0.1.0", wantVersion: Version{0, 1, 0}},
		{vs: "0.0.1", wantVersion: Version{0, 0, 1}},
		{vs: "1.1", wantVersion: Version{1, 1, 0}},
		{vs: "1", wantVersion: Version{1, 0, 0}},
		{vs: "1.2.3.4.5.6", wantVersion: Version{-1, -1, -1}},
		{vs: "a.b.c", wantVersion: Version{-1, -1, -1}},
	}

	for _, tc := range tt {
		v := ParseVersion(tc.vs)

		if v != tc.wantVersion {
			t.Errorf("ParseVersion(%q) = %v; want %v", tc.vs, v, tc.wantVersion)
		}
	}
}

func TestParseVersionErr(t *testing.T) {
	t.Parallel()
	tt := []struct {
		vs          string
		wantVersion Version
		wantErr     string
	}{
		{vs: "1.2.3", wantVersion: Version{1, 2, 3}},
		{vs: "0.0.0", wantVersion: Version{0, 0, 0}},
		{vs: "1.0.0", wantVersion: Version{1, 0, 0}},
		{vs: "0.1.0", wantVersion: Version{0, 1, 0}},
		{vs: "0.0.1", wantVersion: Version{0, 0, 1}},
		{vs: "1.1", wantVersion: Version{1, 1, 0}},
		{vs: "1", wantVersion: Version{1, 0, 0}},
		{vs: "1.2.3.4.5.6", wantErr: "invalid version string: too many dots"},
		{vs: "a.b.c", wantErr: "invalid version string: expected integer"},
	}

	for _, tc := range tt {
		v, err := ParseVersionErr(tc.vs)

		if tc.wantErr != "" {
			if err == nil {
				t.Errorf("ParseVersionErr(%q); want error, but saw none", tc.vs)
			} else if err.Error() != tc.wantErr {
				t.Errorf("ParseVersionErr(%q); want error %q, but got %q", tc.vs, tc.wantErr, err.Error())
			}
		} else {
			if err != nil {
				t.Errorf("ParseVersionErr(%q) = %v; want %v", tc.vs, err, tc.wantVersion)
			} else if v != tc.wantVersion {
				t.Errorf("ParseVersionErr(%q) = %v; want %v", tc.vs, v, tc.wantVersion)
			}
		}
	}
}

func TestVersion_String(t *testing.T) {
	t.Parallel()
	tt := []struct {
		v    Version
		want string
	}{
		{v: Version{1, 2, 3}, want: "1.2.3"},
		{v: Version{0, 0, 0}, want: "0.0.0"},
		{v: Version{1, 0, 0}, want: "1.0.0"},
		{v: Version{0, 1, 0}, want: "0.1.0"},
		{v: Version{0, 0, 1}, want: "0.0.1"},
	}

	for _, tc := range tt {
		if got := tc.v.String(); got != tc.want {
			t.Errorf("%v.String() = %q; want %q", tc.v, got, tc.want)
		}
	}
}

func TestVersion_LessThan(t *testing.T) {
	t.Parallel()
	tt := []struct {
		v     Version
		other Version
		want  bool
	}{
		{v: Version{1, 2, 3}, other: Version{1, 2, 3}, want: false},
		{v: Version{1, 2, 3}, other: Version{1, 2, 4}, want: true},
		{v: Version{1, 2, 3}, other: Version{1, 3, 3}, want: true},
		{v: Version{1, 2, 3}, other: Version{2, 2, 3}, want: true},
		{v: Version{1, 2, 3}, other: Version{1, 2, 2}, want: false},
		{v: Version{1, 2, 3}, other: Version{1, 1, 3}, want: false},
		{v: Version{1, 2, 3}, other: Version{0, 2, 3}, want: false},
	}

	for _, tc := range tt {
		if got := tc.v.LessThan(tc.other); got != tc.want {
			t.Errorf("%v.LessThan(%v) = %v; want %v", tc.v, tc.other, got, tc.want)
		}
	}
}

func TestVersion_Equal(t *testing.T) {
	t.Parallel()
	tt := []struct {
		v     Version
		other Version
		want  bool
	}{
		{v: Version{1, 2, 3}, other: Version{1, 2, 3}, want: true},
		{v: Version{1, 2, 3}, other: Version{1, 2, 4}, want: false},
		{v: Version{1, 2, 3}, other: Version{1, 3, 3}, want: false},
		{v: Version{1, 2, 3}, other: Version{2, 2, 3}, want: false},
		{v: Version{1, 2, 3}, other: Version{1, 2, 2}, want: false},
		{v: Version{1, 2, 3}, other: Version{1, 1, 3}, want: false},
		{v: Version{1, 2, 3}, other: Version{0, 2, 3}, want: false},
	}

	for _, tc := range tt {
		if got := tc.v.Equal(tc.other); got != tc.want {
			t.Errorf("%v.Equal(%v) = %v; want %v", tc.v, tc.other, got, tc.want)
		}
	}
}

func TestVersion_GreaterThan(t *testing.T) {
	t.Parallel()
	tt := []struct {
		v     Version
		other Version
		want  bool
	}{
		{v: Version{1, 2, 3}, other: Version{1, 2, 3}, want: false},
		{v: Version{1, 2, 3}, other: Version{1, 2, 4}, want: false},
		{v: Version{1, 2, 3}, other: Version{1, 3, 3}, want: false},
		{v: Version{1, 2, 3}, other: Version{2, 2, 3}, want: false},
		{v: Version{1, 2, 3}, other: Version{1, 2, 2}, want: true},
		{v: Version{1, 2, 3}, other: Version{1, 1, 3}, want: true},
		{v: Version{1, 2, 3}, other: Version{0, 2, 3}, want: true},
	}

	for _, tc := range tt {
		if got := tc.v.GreaterThan(tc.other); got != tc.want {
			t.Errorf("%v.GreaterThan(%v) = %v; want %v", tc.v, tc.other, got, tc.want)
		}
	}
}
