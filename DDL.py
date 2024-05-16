import duckdb

# File name to store the DuckDB database
TRACK_DATABASE = './trackdb.db'

# Table creation script
CREATE_TABLE = '''
    CREATE TABLE IF NOT EXISTS tracks(
        release_id VARCHAR(36),
        artist_id VARCHAR(36) NOT NULL,
        artist_name VARCHAR(50) NOT NULL,
        track_name VARCHAR(50) NOT NULL,
        release_name VARCHAR(50),
        listened_at TIMESTAMP NOT NULL,
        recording_id VARCHAR(36) NOT NULL,
        user_name VARCHAR(50) NOT NULL,
        PRIMARY KEY (recording_id, user_name, listened_at)
    );
'''

# Connect to the DuckDB database
conn = duckdb.connect(TRACK_DATABASE)

# Create table
conn.execute(CREATE_TABLE)

# Close connection
conn.close()
