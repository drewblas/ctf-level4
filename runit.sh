curl 127.0.0.1:$1/sql -d 'CREATE TABLE hello (world int)'
curl 127.0.0.1:$1/sql -d 'INSERT INTO hello (world) VALUES (1), (2)'
curl 127.0.0.1:$1/sql -d 'SELECT * FROM hello'
