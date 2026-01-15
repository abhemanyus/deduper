#!/bin/bash

sqlite3 jarvis.db "
ATTACH 'hal.db' AS src;
UPDATE files
SET size_bytes = MIN(files.size_bytes, src.files.size_bytes)
SET optimized = COALESCE(files.optimized, src.files.optimized)
FROM src.files
WHERE files.path = src.files.path;
DETACH src;
"

