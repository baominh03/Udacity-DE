
CREATE TABLE IF NOT EXISTS public.us_immgration (
	cic_id FLOAT,
	i94_year FLOAT,
	i94_mode FLOAT,
	i94_port VARCHAR,
	i94_age FLOAT,
	birth_year FLOAT,
	gender VARCHAR(1),
	i94_address VARCHAR,
	i94_visa FLOAT,
	visa_type VARCHAR,
	arrival_date DATE,
	departure_date VARCHAR,
	airport_name VARCHAR,
	state_code VARCHAR,
	state_name VARCHAR,
	airport_city VARCHAR,
	total_population INT,
	PRIMARY KEY (cic_id)
	
);

CREATE TABLE IF NOT EXISTS public.city_demographic (
	city varchar,
	"state" varchar,
	median_age FLOAT,
	male_population INT,
	female_population INT,
	total_Population INT,
	state_code varchar,
	PRIMARY KEY (state_code)
);

CREATE TABLE IF NOT EXISTS public.airport (
	ident varchar,
	"type" varchar,
	"name" varchar,
	continent varchar,
	iso_country varchar,
	municipality varchar,
	iata_code varchar,
	"state" varchar,
	PRIMARY KEY (iata_code)
);


CREATE TABLE IF NOT EXISTS public.staging_immg (
	cic_id FLOAT,
	i94_year FLOAT,
	i94_mode FLOAT,
	i94_port VARCHAR,
	i94_age FLOAT,
	birth_year FLOAT,
	gender VARCHAR(1),
	i94_address VARCHAR,
	i94_visa FLOAT,
	visa_type VARCHAR,
	arrival_date DATE,
	departure_date VARCHAR,
	PRIMARY KEY (cic_id)
	
);

CREATE TABLE IF NOT EXISTS public.staging_city (
	city varchar,
	"state" varchar,
	median_age FLOAT,
	male_population INT,
	female_population INT,
	total_Population INT,
	state_code varchar,
	PRIMARY KEY (state_code)
);

CREATE TABLE IF NOT EXISTS public.staging_airport (
	ident varchar,
	"type" varchar,
	"name" varchar,
	continent varchar,
	iso_country varchar,
	municipality varchar,
	iata_code varchar,
	"state" varchar,
	PRIMARY KEY (iata_code)
);

