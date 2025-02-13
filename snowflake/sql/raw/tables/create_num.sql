CREATE OR REPLACE TABLE NUM (
    adsh VARCHAR(20) NOT NULL,
    tag VARCHAR(256) NOT NULL,
    version VARCHAR(20) NOT NULL,
    ddate DATE NOT NULL,
    qtrs INT NOT NULL,
    uom VARCHAR(20) NOT NULL,
    segments TEXT,
    coreg VARCHAR(256),
    value DECIMAL(28,4),
    footnote TEXT
);