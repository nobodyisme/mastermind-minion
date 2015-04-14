CREATE INDEX IF NOT EXISTS commands_start_ts_idx ON commands (start_ts ASC);
CREATE INDEX IF NOT EXISTS commands_finish_ts_idx ON commands (finish_ts ASC);
