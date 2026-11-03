package keys

import "testing"

func TestValidateQueue(t *testing.T) {
	for _, ok := range []string{"emails", "a", "q-1_x.y"} {
		if err := ValidateQueue(ok); err != nil {
			t.Errorf("ValidateQueue(%q) = %v", ok, err)
		}
	}
	long := make([]byte, 65)
	for i := range long {
		long[i] = 'a'
	}
	for _, bad := range []string{"", "a{b}", "a:b", "a b", string(long)} {
		if ValidateQueue(bad) == nil {
			t.Errorf("ValidateQueue(%q) accepted", bad)
		}
	}
}

func TestKeysShareHashTag(t *testing.T) {
	if got := Job("q", "1"); got != "taskq:{q}:job:1" {
		t.Fatal(got)
	}
	if got := Stream("q", "default"); got != "taskq:{q}:s:default" {
		t.Fatal(got)
	}
}
