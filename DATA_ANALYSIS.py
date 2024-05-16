import duckdb
from tabulate import tabulate

# SQL query to get the top 10 users with respect to the number of songs listened to
SQL_QUERY_A1 = """
SELECT
    user_name, COUNT(track_name) as num_songs_listened
FROM
    tracks
GROUP BY
    user_name
ORDER BY
    num_songs_listened DESC
LIMIT 10
"""


# SQL query to count the number of users who listened to some song on the 1st of March 2019
SQL_QUERY_A2 = """
SELECT
    COUNT(DISTINCT user_name) AS num_users_listened
FROM
    tracks
WHERE
    CAST(listened_at AS DATE) = '2019-03-01'
"""

# SQL query to find the first song each user listened to
SQL_QUERY_A3 = """
SELECT
    user_name, MIN(listened_at) AS first_listen_date, track_name
FROM
    tracks
GROUP BY
    user_name, track_name
ORDER BY
    first_listen_date ASC
LIMIT 10
"""

# SQL query to find the top 3 days with the most listens for each user
SQL_QUERY_B = """
WITH RankedListenedAT AS (
    SELECT
        user_name, CAST(listened_at AS DATE) as listening, COUNT(track_name) AS Count,
        ROW_NUMBER() OVER (PARTITION BY user_name ORDER BY COUNT DESC) AS Rank
    FROM
        tracks
    GROUP BY
        user_name, CAST(listened_at AS DATE)
)
SELECT
    user_name, listening, Count
FROM
    RankedListenedAt
WHERE
    Rank <= 3
"""

# Calculates, on a daily basis, the absolute number of active users, and the percentage of active users among all users
SQL_QUERY_C = """
WITH total_users_count AS(
    SELECT
        CAST(listened_at AS DATE) AS date,
        (SELECT
            COUNT(DISTINCT user_name) 
         FROM
            tracks) AS total_users
    FROM
        tracks
    GROUP BY
        CAST(listened_at AS DATE)
    ORDER BY
        CAST(listened_at AS DATE)
),
active_users_count AS (
    SELECT DISTINCT
        CAST(t1.listened_at AS DATE) AS date,
        COUNT(DISTINCT t1.user_name) AS active_users
    FROM
        tracks t1
    LEFT JOIN 
        tracks t2 ON t1.user_name != t2.user_name
        AND ABS(DATEDIFF('day', CAST(t1.listened_at AS DATE), CAST(t2.listened_at AS DATE))) <= 6
        AND CAST(t1.listened_at AS DATE) > CAST(t2.listened_at AS DATE)
    GROUP BY
        CAST(t1.listened_at AS DATE)
    ORDER BY
        CAST(t1.listened_at AS DATE)
)
SELECT
    auc.date,
    auc.active_users AS number_active_users,
    ROUND((auc.active_users / tuc.total_users) * 100, 2) AS percentage_active_users
FROM
    active_users_count auc
JOIN
    total_users_count tuc ON auc.date = tuc.date 
ORDER BY
    auc.date   
"""

# File name to store the DuckDB database
TRACK_DATABASE = './trackdb.db'

# Connect to the DuckDB database
conn = duckdb.connect(TRACK_DATABASE)

# Run query 
results_A1 = conn.execute(SQL_QUERY_A1)
# Print query results
print('Top 10 users with respect to the number of songs listened to\n')
# Define column headers
headers = ['User Name', 'Number of songs']
# Print results as a table
print(tabulate(results_A1.fetchall(), headers=headers, tablefmt='grid'))
print('\n')

# Run query 
results_A2 = conn.execute(SQL_QUERY_A2)
# Print query results
print('Number of users who listened to some song on the 1st of March 2019\n')
# Define column headers
headers = ['Number of users', 'Number of songs']
print(tabulate(results_A2.fetchall(), headers=headers, tablefmt='grid'))
print('\n')

# Run query 
results_A3 = conn.execute(SQL_QUERY_A3)
# Print query results
print('First song each user listened to\n')
# Define column headers
headers = ['User name', 'Listened at', 'Track name']
print(tabulate(results_A3.fetchall(), headers=headers, tablefmt='grid'))
print('\n')

# Run query 
results_B = conn.execute(SQL_QUERY_B)
# Print query results
print('Top 3 days with the most listens for each user\n')
# Define column headers
headers = ['User name', 'Listened at', 'Number of listens']
print(tabulate(results_B.fetchall(), headers=headers, tablefmt='grid'))
print('\n')

# Run query 
results_C = conn.execute(SQL_QUERY_C)
# Print query results
print('Calculates, on a daily basis, the absolute number of active users, and the percentage of active users among all users\n')
# Define column headers
headers = ['Date', 'Number of active users', 'Percentage of active users']
print(tabulate(results_C.fetchall(), headers=headers, tablefmt='grid'))
