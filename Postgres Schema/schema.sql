CREATE TABLE quotes(
	id SERIAL,
	author_name VARCHAR NOT NULL,
	text VARCHAR NOT NULL,
	PRIMARY KEY (id)
);

CREATE TABLE author(
	author_id SERIAL,
	author_name VARCHAR,
	born VARCHAR,
	description VARCHAR,
	PRIMARY KEY (author_id)
);

CREATE TABLE tag(
	quote_id SERIAL,
	tag VARCHAR ARRAY,
	PRIMARY KEY (quote_id)
);