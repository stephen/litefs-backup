-- migrate:up
CREATE TABLE databases (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	cluster TEXT NOT NULL,
	name TEXT NOT NULL,
	hwm INTEGER NOT NULL DEFAULT 0,
	UNIQUE (cluster, name)
) STRICT;

CREATE TABLE txns (
	db_id INTEGER NOT NULL,
	min_txid INTEGER NOT NULL,
	max_txid INTEGER NOT NULL,
	page_size INTEGER NOT NULL,
	"commit" INTEGER NOT NULL,
	timestamp TEXT NOT NULL,
	pre_apply_checksum BLOB,
	post_apply_checksum BLOB NOT NULL,

	PRIMARY KEY (db_id, min_txid, max_txid),
	CONSTRAINT fk_txns_db_id FOREIGN KEY (db_id) REFERENCES databases (id) ON DELETE CASCADE
) STRICT;

CREATE TABLE pages (
	db_id INTEGER NOT NULL,
	pgno INTEGER NOT NULL,
	min_txid INTEGER NOT NULL,
	max_txid INTEGER NOT NULL,
	chksum BLOB NOT NULL,
	nonce BLOB,
	tag BLOB,
	data BLOB NOT NULL,

	PRIMARY KEY (db_id, pgno, max_txid, min_txid)
	CONSTRAINT fk_pages_db_id FOREIGN KEY (db_id) REFERENCES databases (id) ON DELETE CASCADE
) STRICT;

CREATE TABLE compaction_requests (
	db_id INTEGER NOT NULL,
	level INTEGER NOT NULL,
	idempotency_key INTEGER NOT NULL,
	PRIMARY KEY (db_id, level),
	CONSTRAINT fk_compaction_requests_db_id FOREIGN KEY (db_id) REFERENCES databases (id) ON DELETE CASCADE
) STRICT;


-- migrate:down
DROP TABLE databases;
DROP TABLE txns;
DROP TABLE pages;
DROP TABLE compaction_requests;
