CREATE TABLE crypto_ind_test (
    time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    currency TEXT NOT NULL,
    close_minmax_scale DOUBLE PRECISION,
    close DOUBLE PRECISION,
    ma25_99h DOUBLE PRECISION,
    ma7_25h DOUBLE PRECISION,
    ma7_25d DOUBLE PRECISION,
    ma25_99h_scale DOUBLE PRECISION,
    ma7_25h_scale DOUBLE PRECISION,
    ma7_25d_scale DOUBLE PRECISION,
    PRIMARY KEY (time, currency)
);
