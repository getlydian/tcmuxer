package tcmux

import (
	"reflect"
	"testing"
)

type collision struct {
	path string
	ns   string
}

func recorder() (*[]collision, CollisionFunc) {
	var got []collision
	return &got, func(path, ns string) {
		got = append(got, collision{path, ns})
	}
}

func TestMerge_EmptyDst(t *testing.T) {
	dst := map[string]any{}
	src := map[string]any{
		"http": map[string]any{
			"routers": map[string]any{
				"a": map[string]any{"rule": "Host(`a`)"},
			},
		},
	}
	got, fn := recorder()
	Merge(dst, src, "ns", fn)
	if !reflect.DeepEqual(dst, src) {
		t.Fatalf("dst = %#v, want %#v", dst, src)
	}
	if len(*got) != 0 {
		t.Fatalf("unexpected collisions: %#v", *got)
	}
}

func TestMerge_NestedRouters(t *testing.T) {
	dst := map[string]any{
		"http": map[string]any{
			"routers": map[string]any{
				"a": map[string]any{"rule": "Host(`a`)"},
			},
		},
	}
	src := map[string]any{
		"http": map[string]any{
			"routers": map[string]any{
				"b": map[string]any{"rule": "Host(`b`)"},
			},
			"services": map[string]any{
				"svc": map[string]any{"loadBalancer": map[string]any{}},
			},
		},
	}
	got, fn := recorder()
	Merge(dst, src, "ns", fn)

	want := map[string]any{
		"http": map[string]any{
			"routers": map[string]any{
				"a": map[string]any{"rule": "Host(`a`)"},
				"b": map[string]any{"rule": "Host(`b`)"},
			},
			"services": map[string]any{
				"svc": map[string]any{"loadBalancer": map[string]any{}},
			},
		},
	}
	if !reflect.DeepEqual(dst, want) {
		t.Fatalf("dst = %#v, want %#v", dst, want)
	}
	if len(*got) != 0 {
		t.Fatalf("unexpected collisions: %#v", *got)
	}
}

func TestMerge_ConcatLists(t *testing.T) {
	dst := map[string]any{
		"tls": map[string]any{
			"certificates": []any{
				map[string]any{"certFile": "a.crt"},
			},
		},
	}
	src := map[string]any{
		"tls": map[string]any{
			"certificates": []any{
				map[string]any{"certFile": "b.crt"},
				map[string]any{"certFile": "c.crt"},
			},
		},
	}
	_, fn := recorder()
	Merge(dst, src, "ns", fn)

	certs := dst["tls"].(map[string]any)["certificates"].([]any)
	if len(certs) != 3 {
		t.Fatalf("len(certs) = %d, want 3", len(certs))
	}
	if certs[0].(map[string]any)["certFile"] != "a.crt" ||
		certs[1].(map[string]any)["certFile"] != "b.crt" ||
		certs[2].(map[string]any)["certFile"] != "c.crt" {
		t.Fatalf("concat order wrong: %#v", certs)
	}
}

func TestMerge_ScalarCollision_ExistingWins(t *testing.T) {
	dst := map[string]any{
		"http": map[string]any{
			"routers": map[string]any{
				"shared": map[string]any{"rule": "Host(`first`)"},
			},
		},
	}
	src := map[string]any{
		"http": map[string]any{
			"routers": map[string]any{
				"shared": map[string]any{"rule": "Host(`second`)"},
			},
		},
	}
	got, fn := recorder()
	Merge(dst, src, "loser", fn)

	rule := dst["http"].(map[string]any)["routers"].(map[string]any)["shared"].(map[string]any)["rule"]
	if rule != "Host(`first`)" {
		t.Fatalf("rule = %q, want first to win", rule)
	}
	if len(*got) != 1 {
		t.Fatalf("got %d collisions, want 1: %#v", len(*got), *got)
	}
	if (*got)[0] != (collision{"http.routers.shared.rule", "loser"}) {
		t.Fatalf("collision = %#v, want path=http.routers.shared.rule ns=loser", (*got)[0])
	}
}

func TestMerge_TypeMismatch_ExistingWins(t *testing.T) {
	dst := map[string]any{
		"http": map[string]any{
			"routers": map[string]any{"a": map[string]any{}},
		},
	}
	src := map[string]any{
		"http": map[string]any{
			"routers": "not-a-map",
		},
	}
	got, fn := recorder()
	Merge(dst, src, "loser", fn)

	if _, ok := dst["http"].(map[string]any)["routers"].(map[string]any); !ok {
		t.Fatalf("routers should still be a map, got %T", dst["http"].(map[string]any)["routers"])
	}
	if len(*got) != 1 || (*got)[0].path != "http.routers" || (*got)[0].ns != "loser" {
		t.Fatalf("collisions = %#v, want one at http.routers/loser", *got)
	}
}

func TestMerge_MapVsList_Collision(t *testing.T) {
	dst := map[string]any{"k": []any{1, 2}}
	src := map[string]any{"k": map[string]any{"a": 1}}
	got, fn := recorder()
	Merge(dst, src, "loser", fn)

	if !reflect.DeepEqual(dst["k"], []any{1, 2}) {
		t.Fatalf("dst[k] = %#v, want existing list to win", dst["k"])
	}
	if len(*got) != 1 || (*got)[0] != (collision{"k", "loser"}) {
		t.Fatalf("collisions = %#v, want one at k/loser", *got)
	}
}

func TestMerge_NilCallback(t *testing.T) {
	dst := map[string]any{"k": "first"}
	src := map[string]any{"k": "second"}
	// Must not panic when callback is nil.
	Merge(dst, src, "loser", nil)
	if dst["k"] != "first" {
		t.Fatalf("dst[k] = %v, want first", dst["k"])
	}
}
