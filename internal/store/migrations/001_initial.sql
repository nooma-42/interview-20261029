CREATE TABLE rockets (
 channel text PRIMARY KEY,
 rocket_type text,
 speed bigint,
 mission text,
 status text NOT NULL DEFAULT 'awaiting_launch' CHECK (status IN ('awaiting_launch','launched','exploded')),
 explosion_reason text,
 last_message_time timestamptz,
 last_applied bigint NOT NULL DEFAULT 0 CHECK (last_applied >= 0),
 max_received bigint NOT NULL DEFAULT 0 CHECK (max_received >= last_applied),
 received_count bigint NOT NULL DEFAULT 0 CHECK (received_count >= last_applied)
);
CREATE TABLE rocket_events (
 channel text NOT NULL REFERENCES rockets(channel),
 message_number bigint NOT NULL CHECK (message_number > 0),
 event_type text NOT NULL,
 message_time timestamptz NOT NULL,
 payload jsonb NOT NULL,
 PRIMARY KEY (channel, message_number)
);
CREATE INDEX rockets_speed_idx ON rockets (speed, channel);
CREATE INDEX rockets_mission_idx ON rockets (mission, channel);
CREATE INDEX rockets_time_idx ON rockets (last_message_time, channel);
