package peers

import "testing"

func TestRegistryExcludesSelf(t *testing.T) {
	r := NewRegistry("http://self:8080", []string{"http://self:8080", "http://a:8080"})
	got := r.List()
	if len(got) != 1 || got[0] != "http://a:8080" {
		t.Fatalf("List() = %v, want [http://a:8080]", got)
	}
}

func TestRegistrySetReplacesList(t *testing.T) {
	r := NewRegistry("", []string{"http://a:8080"})
	r.Set([]string{"http://b:8080", "http://c:8080"})

	got := r.List()
	if len(got) != 2 || got[0] != "http://b:8080" || got[1] != "http://c:8080" {
		t.Fatalf("List() = %v, want [http://b:8080 http://c:8080]", got)
	}
}

func TestRegistryListReturnsIndependentCopy(t *testing.T) {
	r := NewRegistry("", []string{"http://a:8080"})
	got := r.List()
	got[0] = "mutated"

	if r.List()[0] != "http://a:8080" {
		t.Fatal("mutating the returned slice must not affect the registry's internal state")
	}
}
