SELECT *
FROM "logs-hotel-search-2025.08.09"
LIMIT 10;


--

SHOW CATALOGS;


SHOW SCHEMAS FROM elasticsearch;


connector.name=elasticsearch
elasticsearch.host=localhost
elasticsearch.port=9200
elasticsearch.default-schema-name=default
