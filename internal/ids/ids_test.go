package ids

import "testing"

func TestNewUUID(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := NewUUID()
		if !IsUUID(id) {
			t.Fatalf("generated id %q is not a canonical UUID", id)
		}
		if seen[id] {
			t.Fatalf("duplicate UUID %q", id)
		}
		seen[id] = true
	}
}

func TestIsUUID(t *testing.T) {
	valid := []string{
		"11111111-2222-3333-4444-555555555555",
		"abcdef01-2345-6789-abcd-ef0123456789",
		"ABCDEF01-2345-6789-ABCD-EF0123456789",
	}
	for _, id := range valid {
		if !IsUUID(id) {
			t.Fatalf("IsUUID(%q) = false", id)
		}
	}
	invalid := []string{
		"", "not-a-uuid", "11111111222233334444555555555555",
		"11111111-2222-3333-4444-55555555555",   // short
		"11111111-2222-3333-4444-5555555555555", // long
		"11111111-2222-3333-4444-55555555555g",  // non-hex
		"11111111_2222_3333_4444_555555555555",
	}
	for _, id := range invalid {
		if IsUUID(id) {
			t.Fatalf("IsUUID(%q) = true", id)
		}
	}
}
