package main

import "testing"

func TestSubjectsMatch(t *testing.T) {
	// Test that the subjects match
	if SubjectsMatch("foo.bar", "foo.bar") == false {
		t.Errorf("Expected subject to match")
	}
	if SubjectsMatch("foo.bar", "foo.*") == false {
		t.Errorf("Expected subject to match")
	}
	if SubjectsMatch("foo.bar", "foo.>") == false {
		t.Errorf("Expected subject to match")
	}
	if SubjectsMatch("foo.bar", ">") == false {
		t.Errorf("Expected subject to match")
	}

	// Test that the subjects do not match
	if SubjectsMatch("foo.bar", "bar.>") == true {
		t.Errorf("Expected subject to not match")
	}
}
