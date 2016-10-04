
ALTER TABLE commands
    ADD stdout text DEFAULT "";

ALTER TABLE commands
    ADD stderr text DEFAULT "";

ALTER TABLE commands
    ADD update_ts integer DEFAULT NULL;
