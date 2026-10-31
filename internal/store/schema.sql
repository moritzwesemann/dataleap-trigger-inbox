CREATE TABLE IF NOT EXISTS events (
  id           bigserial   PRIMARY KEY,
  agent_id     text        NOT NULL,
  event_id     text        NOT NULL,
  payload      jsonb       NOT NULL,
  status       text        NOT NULL DEFAULT 'waiting'
                           CHECK (status IN ('waiting', 'running', 'done', 'failed')),
  attempts     int         NOT NULL DEFAULT 0,
  locked_until timestamptz,
  last_error   text,
  received_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (agent_id, event_id)
);

CREATE INDEX IF NOT EXISTS events_waiting ON events (agent_id, id) WHERE status = 'waiting';

CREATE INDEX IF NOT EXISTS events_reserved ON events (locked_until) WHERE status = 'running';

CREATE INDEX IF NOT EXISTS events_failed ON events (agent_id, id) WHERE status = 'failed';
