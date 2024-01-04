CREATE TABLE Businesses (
	FormattedLegalBusinessName TEXT,
	FormattedBusinessAddress TEXT,
	RawLegalBusinessName TEXT,
	RawBusinessAddress TEXT,
	BranchID VARCHAR(16),
	PRIMARY KEY (FormattedLegalBusinessName, FormattedBusinessAddress)
);

COPY Businesses
FROM '/docker-entrypoint-initdb.d/businesses.csv'
DELIMITER ','
CSV HEADER;