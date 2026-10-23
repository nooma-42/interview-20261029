package domain

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

func event(t *testing.T, n int64, kind, payload string) Event {
	t.Helper()
	raw := fmt.Sprintf(`{"metadata":{"channel":"a","messageNumber":%d,"messageTime":"2022-02-02T18:39:05.863370123Z","messageType":%q},"message":%s}`, n, kind, payload)
	e, err := Parse([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func TestTransitions(t *testing.T) {
	r := NewRocket("a")
	cases := []struct {
		kind, payload           string
		speed                   int64
		mission, status, reason string
	}{
		{Launched, `{"type":"Falcon-9","launchSpeed":500,"mission":"ARTEMIS"}`, 500, "ARTEMIS", "launched", ""},
		{SpeedIncreased, `{"by":3000}`, 3500, "ARTEMIS", "launched", ""},
		{SpeedDecreased, `{"by":4000}`, -500, "ARTEMIS", "launched", ""},
		{MissionChanged, `{"newMission":"SHUTTLE_MIR"}`, -500, "SHUTTLE_MIR", "launched", ""},
		{Exploded, `{"reason":"FAILURE"}`, -500, "SHUTTLE_MIR", "exploded", "FAILURE"},
		{SpeedIncreased, `{"by":100}`, -500, "SHUTTLE_MIR", "exploded", "FAILURE"},
		{MissionChanged, `{"newMission":"IGNORED"}`, -500, "SHUTTLE_MIR", "exploded", "FAILURE"},
		{Exploded, `{"reason":"IGNORED"}`, -500, "SHUTTLE_MIR", "exploded", "FAILURE"},
	}
	for i, c := range cases {
		e := event(t, int64(i+1), c.kind, c.payload)
		var err error
		r, err = Apply(r, e)
		if err != nil {
			t.Fatal(err)
		}
		if *r.Type != "Falcon-9" || *r.Speed != c.speed || *r.Mission != c.mission || r.Status != c.status || r.LastApplied != int64(i+1) || !r.LastMessageTime.Equal(e.Metadata.Time) {
			t.Fatalf("step %d: %+v", i, r)
		}
		if c.reason != "" && (r.ExplosionReason == nil || *r.ExplosionReason != c.reason) {
			t.Fatal("explosion reason changed")
		}
	}
}
func TestValidation(t *testing.T) {
	valid := string(event(t, 1, Launched, `{"type":"x","launchSpeed":0,"mission":"m"}`).JSON())
	cases := []string{"", `null`, `[]`, valid + ` {}`, strings.Replace(valid, `"channel":"a"`, `"channel":""`, 1), strings.Replace(valid, `"messageNumber":1`, `"messageNumber":0`, 1), strings.Replace(valid, `"messageNumber":1`, `"messageNumber":2`, 1), strings.Replace(valid, `"messageNumber":1`, `"messageNumber":1.5`, 1), strings.Replace(valid, `"launchSpeed":0`, `"launchSpeed":-1`, 1), strings.Replace(valid, `"launchSpeed":0`, `"launchSpeed":null`, 1), strings.Replace(valid, `"launchSpeed":0,`, ``, 1), strings.Replace(valid, `"mission":"m"`, `"mission":null`, 1), strings.Replace(valid, `"mission":"m"`, `"mission":" "`, 1), strings.Replace(valid, `RocketLaunched`, `Unknown`, 1), strings.Replace(valid, `2022-02-02T18:39:05.863370123Z`, `not-a-date`, 1), strings.Replace(valid, `"type":"x"`, `"extra":1,"type":"x"`, 1), strings.Replace(valid, `"channel":"a"`, `"channel":"a\u0000b"`, 1)}
	for i, raw := range cases {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Errorf("case %d accepted: %s", i, raw)
		}
	}
	for _, c := range []struct{ kind, payload string }{{SpeedIncreased, `{}`}, {SpeedDecreased, `{"by":-1}`}, {SpeedIncreased, `{"by":9223372036854775808}`}, {SpeedIncreased, `{"by":1.2}`}, {Exploded, `{"reason":""}`}, {MissionChanged, `{"newMission":null}`}, {SpeedDecreased, `null`}} {
		raw := fmt.Sprintf(`{"metadata":{"channel":"a","messageNumber":2,"messageTime":"2022-01-01T00:00:00Z","messageType":%q},"message":%s}`, c.kind, c.payload)
		if _, err := Parse([]byte(raw)); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
}
func TestOverflowAndSequence(t *testing.T) {
	r, err := Apply(NewRocket("a"), event(t, 1, Launched, fmt.Sprintf(`{"type":"x","launchSpeed":%d,"mission":"m"}`, int64(math.MaxInt64))))
	if err != nil {
		t.Fatal(err)
	}
	after, err := Apply(r, event(t, 2, SpeedIncreased, `{"by":1}`))
	if !errors.Is(err, ErrOverflow) || *after.Speed != math.MaxInt64 || after.LastApplied != 1 {
		t.Fatal("overflow mutated state", after, err)
	}
	speed := int64(math.MinInt64)
	r.Speed = &speed
	_, err = Apply(r, event(t, 2, SpeedDecreased, `{"by":1}`))
	if !errors.Is(err, ErrOverflow) {
		t.Fatal(err)
	}
	if _, err = Apply(r, event(t, 3, SpeedIncreased, `{"by":0}`)); err == nil {
		t.Fatal("accepted gap")
	}
	wrong := event(t, 2, SpeedIncreased, `{"by":0}`)
	wrong.Metadata.Channel = "b"
	if _, err = Apply(r, wrong); err == nil {
		t.Fatal("accepted different channel")
	}
}
func TestCanonicalEvent(t *testing.T) {
	a := event(t, 1, Launched, `{"mission":"m","launchSpeed":0,"type":"x"}`)
	b, err := Parse([]byte(strings.Replace(string(a.JSON()), "18:39:05.863370123Z", "19:39:05.863370123+01:00", 1)))
	if err != nil || string(a.JSON()) != string(b.JSON()) {
		t.Fatal("equivalent instant must normalize", err)
	}
}

func TestUnrepresentableUTCAndInvalidUTF8(t *testing.T) {
	valid := string(event(t, 1, Launched, `{"type":"x","launchSpeed":0,"mission":"m"}`).JSON())
	for _, timestamp := range []string{"9999-12-31T23:59:59-01:00", "0000-01-01T00:00:00+01:00"} {
		raw := strings.Replace(valid, "2022-02-02T18:39:05.863370123Z", timestamp, 1)
		if _, err := Parse([]byte(raw)); err == nil {
			t.Fatal("accepted unrepresentable UTC time", timestamp)
		}
	}
	raw := strings.Replace(valid, `"channel":"a"`, "\"channel\":\""+string([]byte{255})+"\"", 1)
	if _, err := Parse([]byte(raw)); err == nil {
		t.Fatal("accepted invalid UTF-8")
	}
}
