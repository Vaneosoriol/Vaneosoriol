-- vane_carop19_coderhouse.info_marvel definition

-- Drop table

-- DROP TABLE vane_carop19_coderhouse.info_marvel;

--DROP TABLE vane_carop19_coderhouse.info_marvel;
CREATE TABLE IF NOT EXISTS vane_carop19_coderhouse.info_marvel
(
	id INTEGER   ENCODE az64
	,name VARCHAR(255)   ENCODE lzo
	,description VARCHAR(256)   ENCODE lzo
	,modified TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,resourceuri VARCHAR(255)   ENCODE lzo
	,urls VARCHAR(255)   ENCODE lzo
	,thumbnail_path VARCHAR(255)   ENCODE lzo
	,thumbnail_extension VARCHAR(255)   ENCODE lzo
	,comics_available INTEGER   ENCODE az64
	,comics_collectionuri VARCHAR(255)   ENCODE lzo
	,comics_items VARCHAR(65535)   ENCODE lzo
	,comics_returned INTEGER   ENCODE az64
	,series_available INTEGER   ENCODE az64
	,series_collectionuri VARCHAR(255)   ENCODE lzo
	,series_items VARCHAR(65535)   ENCODE lzo
	,series_returned INTEGER   ENCODE az64
	,stories_available INTEGER   ENCODE az64
	,stories_collectionuri VARCHAR(255)   ENCODE lzo
	,stories_items VARCHAR(65535)   ENCODE lzo
	,stories_returned INTEGER   ENCODE az64
	,events_available INTEGER   ENCODE az64
	,events_collectionuri VARCHAR(255)   ENCODE lzo
	,events_items VARCHAR(65535)   ENCODE lzo
	,events_returned INTEGER   ENCODE az64
	,has_series_available VARCHAR(2)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE vane_carop19_coderhouse.info_marvel owner to vane_carop19_coderhouse;