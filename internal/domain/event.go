package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	Launched       = "RocketLaunched"
	SpeedIncreased = "RocketSpeedIncreased"
	SpeedDecreased = "RocketSpeedDecreased"
	Exploded       = "RocketExploded"
	MissionChanged = "RocketMissionChanged"
)

var ErrOverflow = errors.New("speed exceeds int64 range")

type Metadata struct {
	Channel string    `json:"channel"`
	Number  int64     `json:"messageNumber"`
	Time    time.Time `json:"messageTime"`
	Type    string    `json:"messageType"`
}

type Launch struct {
	Type    string `json:"type"`
	Speed   *int64 `json:"launchSpeed"`
	Mission string `json:"mission"`
}
type Increase struct {
	By *int64 `json:"by"`
}
type Decrease struct {
	By *int64 `json:"by"`
}
type Explosion struct {
	Reason string `json:"reason"`
}
type MissionChange struct {
	Mission string `json:"newMission"`
}

// Event contains one of the five typed payloads. Parse is the validation boundary.
type Event struct {
	Metadata Metadata `json:"metadata"`
	Message  any      `json:"message"`
}

func decode(data []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("expected one JSON object")
	}
	return nil
}

func Parse(data []byte) (Event, error) {
	if !utf8.Valid(data) {
		return Event{}, errors.New("JSON must use valid UTF-8")
	}
	var wire struct {
		Metadata Metadata        `json:"metadata"`
		Message  json.RawMessage `json:"message"`
	}
	if err := decode(data, &wire); err != nil {
		return Event{}, fmt.Errorf("invalid message: %w", err)
	}
	m := wire.Metadata
	if strings.TrimSpace(m.Channel) == "" || len(m.Channel) > 255 || m.Number < 1 || m.Time.IsZero() {
		return Event{}, errors.New("channel (1-255 bytes), positive messageNumber and RFC3339 messageTime are required")
	}
	// PostgreSQL text cannot contain NUL. Keep this a client error at the boundary.
	if strings.ContainsRune(m.Channel, 0) {
		return Event{}, errors.New("channel must not contain NUL")
	}
	m.Time = m.Time.UTC()
	if m.Time.Year() < 0 || m.Time.Year() > 9999 {
		return Event{}, errors.New("messageTime must be representable in UTC RFC3339")
	}
	var payload any
	switch m.Type {
	case Launched:
		if m.Number != 1 {
			return Event{}, errors.New("RocketLaunched must be messageNumber 1")
		}
		payload = &Launch{}
	case SpeedIncreased:
		payload = &Increase{}
	case SpeedDecreased:
		payload = &Decrease{}
	case Exploded:
		payload = &Explosion{}
	case MissionChanged:
		payload = &MissionChange{}
	default:
		return Event{}, errors.New("unsupported messageType")
	}
	if m.Type != Launched && m.Number == 1 {
		return Event{}, errors.New("messageNumber 1 must be RocketLaunched")
	}
	if len(wire.Message) == 0 || bytes.Equal(bytes.TrimSpace(wire.Message), []byte("null")) {
		return Event{}, errors.New("message object is required")
	}
	if err := decode(wire.Message, payload); err != nil {
		return Event{}, fmt.Errorf("invalid payload: %w", err)
	}
	validText := func(s string) bool { return strings.TrimSpace(s) != "" && !strings.ContainsRune(s, 0) }
	validSpeed := func(n *int64) bool { return n != nil && *n >= 0 }
	valid := false
	switch p := payload.(type) {
	case *Launch:
		valid = validText(p.Type) && validText(p.Mission) && validSpeed(p.Speed)
	case *Increase:
		valid = validSpeed(p.By)
	case *Decrease:
		valid = validSpeed(p.By)
	case *Explosion:
		valid = validText(p.Reason)
	case *MissionChange:
		valid = validText(p.Mission)
	}
	if !valid {
		return Event{}, errors.New("payload is missing a required field or contains an invalid value")
	}
	return Event{Metadata: m, Message: payload}, nil
}

func (e Event) JSON() []byte {
	// Validated event payloads contain only JSON-compatible values.
	data, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	return data
}

type Rocket struct {
	Channel         string     `json:"channel"`
	Type            *string    `json:"type"`
	Speed           *int64     `json:"speed"`
	Mission         *string    `json:"mission"`
	Status          string     `json:"status"`
	ExplosionReason *string    `json:"explosionReason"`
	LastMessageTime *time.Time `json:"lastMessageTime"`
	LastApplied     int64      `json:"lastAppliedMessageNumber"`
	MaxReceived     int64      `json:"maxReceivedMessageNumber"`
	ReceivedCount   int64      `json:"-"`
	Pending         int64      `json:"pendingMessageCount"`
}

func NewRocket(channel string) Rocket { return Rocket{Channel: channel, Status: "awaiting_launch"} }

// Apply returns a new snapshot and leaves the original untouched on any error.
// Sequence numbers, not timestamps, define event order.
func Apply(r Rocket, e Event) (Rocket, error) {
	if e.Metadata.Channel != r.Channel || r.LastApplied == math.MaxInt64 || e.Metadata.Number != r.LastApplied+1 {
		return r, errors.New("event must be the next message for this channel")
	}
	next := r
	if r.Status != "exploded" {
		switch p := e.Message.(type) {
		case *Launch:
			next.Type, next.Speed, next.Mission = &p.Type, p.Speed, &p.Mission
			next.Status = "launched"
		case *Increase:
			if r.Speed == nil {
				return r, errors.New("launch required before speed change")
			}
			if *r.Speed > math.MaxInt64-*p.By {
				return r, ErrOverflow
			}
			speed := *r.Speed + *p.By
			next.Speed = &speed
		case *Decrease:
			if r.Speed == nil {
				return r, errors.New("launch required before speed change")
			}
			if *r.Speed < math.MinInt64+*p.By {
				return r, ErrOverflow
			}
			speed := *r.Speed - *p.By
			next.Speed = &speed
		case *MissionChange:
			next.Mission = &p.Mission
		case *Explosion:
			next.Status, next.ExplosionReason = "exploded", &p.Reason
		default:
			return r, errors.New("unsupported event payload")
		}
	}
	next.LastApplied = e.Metadata.Number
	next.LastMessageTime = &e.Metadata.Time
	return next, nil
}
