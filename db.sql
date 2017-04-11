CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE systems (
	id uuid DEFAULT uuid_generate_v4() NOT NULL,
	name character varying NOT NULL UNIQUE,
	commited_at timestamp with time zone DEFAULT current_timestamp NOT NULL,
	PRIMARY KEY (id)
);

CREATE TABLE files (
	id uuid DEFAULT uuid_generate_v4() NOT NULL,
	system_id uuid NOT NULL REFERENCES systems(id) ON DELETE RESTRICT ON UPDATE CASCADE,
	path character varying NOT NULL,
	mode integer NOT NULL,
	uid integer NOT NULL,
	gid integer NOT NULL,
	ctime timestamp with time zone NOT NULL,
	mtime timestamp with time zone NOT NULL,
	size integer NULL,
	rdev integer NULL,
	blob bytea NULL,
	PRIMARY KEY (id),
	UNIQUE (system_id, path)
);
