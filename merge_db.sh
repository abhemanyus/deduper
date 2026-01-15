#!/bin/bash

sqlite3 jarvis.db "
ATTACH 'hal.db' AS src;
INSERT OR IGNORE INTO files SELECT * FROM src.files;
DETACH src;
"

