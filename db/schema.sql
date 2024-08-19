CREATE TABLE IF NOT EXISTS "schema_migrations" (version varchar(255) primary key);
CREATE TABLE databases (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	org_id INTEGER NOT NULL,
	cluster TEXT NOT NULL,
	name TEXT NOT NULL,
	hwm INTEGER NOT NULL DEFAULT 0,
	UNIQUE (org_id, cluster, name)
) STRICT;
CREATE TABLE sqlite_sequence(name,seq);
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
-- Dbmate schema migrations
INSERT INTO "schema_migrations" (version) VALUES
  ('20240819023925');
