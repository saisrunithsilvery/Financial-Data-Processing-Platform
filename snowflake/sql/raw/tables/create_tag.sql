CREATE OR REPLACE TABLE TAG (
    tag VARCHAR(256) NOT NULL,
    version VARCHAR(20) NOT NULL,
    custom BOOLEAN NOT NULL,
    abstract BOOLEAN NOT NULL,
    datatype VARCHAR(20),
    iord CHAR(1),
    crdr CHAR(1),
    tlabel VARCHAR(512),
    doc TEXT
);