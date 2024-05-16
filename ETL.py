import pandas as pd
import json
import logging
import duckdb
import datetime


def transform(chunk: pd.DataFrame):

    # Normalize records - flatten track_metadata structure
    nrmlsd_chunk = pd.json_normalize(json.loads(chunk.to_json(orient="records")))

    # Mapping of columns from Source to Target
    columns_to_drop = [
        "track_metadata.additional_info.recording_msid",
        "track_metadata.additional_info.track_mbid",
        "track_metadata.additional_info.release_mbid",
        "track_metadata.additional_info.recording_mbid",
        "track_metadata.additional_info.release_group_mbid",
        "track_metadata.additional_info.artist_mbids",
        "track_metadata.additional_info.tags",
        "track_metadata.additional_info.work_mbids",
        "track_metadata.additional_info.isrc",
        "track_metadata.additional_info.spotify_id",
        "track_metadata.additional_info.tracknumber"
        ]
    
    nrmlsd_chunk = nrmlsd_chunk.drop(columns_to_drop, axis=1)

    nrmlsd_chunk = nrmlsd_chunk.rename(columns={
        "track_metadata.additional_info.release_msid": "release_id",
        "track_metadata.additional_info.artist_msid": "artist_id",
        "track_metadata.artist_name": "artist_name",
        "track_metadata.track_name": "track_name",       
        "track_metadata.release_name": "release_name",
        "recording_msid": "recording_id"
    })

    # Redefine the order of columns
    nrmlsd_chunk = nrmlsd_chunk[[
        'release_id',
        'artist_id', 
        'artist_name',
        'track_name',
        'release_name',
        'listened_at',
        'recording_id',
        'user_name'
     ]]  

    # Clean data
    # Trim whitespace
    nrmlsd_chunk['user_name'] = nrmlsd_chunk['user_name'].str.strip()
    nrmlsd_chunk['recording_id'] = nrmlsd_chunk['recording_id'].str.strip()
    nrmlsd_chunk['release_name'] = nrmlsd_chunk['release_name'].str.strip()
    nrmlsd_chunk['track_name'] = nrmlsd_chunk['track_name'].str.strip()
    nrmlsd_chunk['artist_name'] = nrmlsd_chunk['artist_name'].str.strip()
    nrmlsd_chunk['artist_id'] = nrmlsd_chunk['artist_id'].str.strip()
    nrmlsd_chunk['release_id'] = nrmlsd_chunk['release_id'].str.strip()

    # Transformation of unix time in milliseconds to human readable
    nrmlsd_chunk['listened_at'] = pd.to_datetime(nrmlsd_chunk['listened_at'], unit='ms')

    # Produce list of records to be loaded
    records = nrmlsd_chunk.values.tolist()

    return records

def load(conn, records):

    # Load records in bulk and ignore duplicates that violate the uniqueness contraint of the primary key columns
    sql = '''
        INSERT INTO tracks (release_id, artist_id, artist_name, track_name, release_name, listened_at, recording_id, user_name)    
        VALUES (?, ?, ?, ?, ?, ?, ?, ?) 
    '''
    
    try:
        if records:
            conn.executemany(sql, records)

    except duckdb.Error as e:
        if  "Duplicate key" in str(e) or "NOT NULL" in str(e) or "Constraint Error":
            logging.info(f'Record rejected {e}')
            return 1
        else:
            logging.error(f'Error occurred: {e}')
            return 0
    return 0

# ETL Pipeline     
logging.basicConfig(filename='etl.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# File name to store the DuckDB database
TRACK_DATABASE = './trackdb.db'

# Connect to the DuckDB database
conn = duckdb.connect(TRACK_DATABASE)

# Chunk size defines the number of lines to read 
CHUNK_SIZE = 1000

# Dataset provided
SOURCE = './dataset/dataset.txt'

# Extract data from Source in chunks
lz_df = pd.read_json(SOURCE, lines=True, chunksize=CHUNK_SIZE)

total = 0
invalid_records = 0

for idx, lz_chunk in enumerate(lz_df):
    # Transform chunks
    records = transform(lz_chunk)
    logging.info(f'Finished processing chunk {idx+1}.')
    invalid_records += sum(load(conn, records) for _ in records)
    logging.info(f'Finished loading chunk {idx+1}.')
    total += len(lz_chunk)

loaded_records = conn.execute('SELECT COUNT(*) FROM tracks').fetchone()[0]
new_records = total - invalid_records

conn.close()

logging.info(f'Number of records processed: {total}')
logging.info(f'Number of records rejected: {invalid_records}')
logging.info(f'Number of new records loaded: {new_records}')
logging.info(f'Number of records in the database: {loaded_records}')

