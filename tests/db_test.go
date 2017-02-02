package tests

import (
	"testing"
	"github.com/uncleandy/tcache2/db"
)

func TestDbEscaped(t *testing.T) {
	str := db.Escaped("Simple string")
	if str != "Simple string" {
		t.Error("Escaped result wrong. Expected \"Simple string\", got:", str)
	}
	str = db.Escaped("Simple ' string")
	if str != "Simple '' string" {
		t.Error("Escaped result wrong. Expected \"Simple '' string\", got:", str)
	}
	str = db.Escaped("'Simple ' string'")
	if str != "''Simple '' string''" {
		t.Error("Escaped result wrong. Expected \"''Simple '' string''\", got:", str)
	}
}

func TestDbConvertTime(t *testing.T) {
	str := db.ConvertTime("2017-01-23T01:02:03Z")
	if str != "2017-01-23 01:02:03" {
		t.Error("Wrong ConvertTime result. Expected '2017-01-23 01:02:03', got:", str)
	}

	str = db.ConvertTime("2017-01-23 01:02:03")
	if str != "2017-01-23 01:02:03" {
		t.Error("Wrong ConvertTime result. Expected '2017-01-23 01:02:03', got:", str)
	}

	str = db.ConvertTime("2017-01-23T00:00:00Z")
	if str != "2017-01-23" {
		t.Error("Wrong ConvertTime result. Expected '2017-01-23', got:", str)
	}

	str = db.ConvertTime("2017-01-23T01:02:03")
	if str != "2017-01-23 01:02:03" {
		t.Error("Wrong ConvertTime result. Expected '2017-01-23 01:02:03', got:", str)
	}
}