CREATE TABLE indicator_log (
    id SERIAL PRIMARY KEY,
	op_time TIMESTAMP WITHOUT TIME ZONE,
    start_date TIMESTAMP WITHOUT TIME ZONE,
    end_date TIMESTAMP WITHOUT TIME ZONE,
    num_row INTEGER,
    havenull BOOLEAN,
    haveinf BOOLEAN,
	tablename TEXT,
	success BOOLEAN
);
