import DbConnector as db
from DBManager import DBManager as dbm
from haversine import haversine, Unit

db = db.DbConnector("localhost", "testdb", "testuser", PASSWORD="test123")
dbm = dbm(db)

# dbm.init_Users()
# dbm.init_Activity()
# dbm.init_TrackPoint()
# dbm.create_users()

# dbm.create_activities()
# dbm.test_stuff()

# results = dbm.get_labels("010")
# for r in results:
#     print(r)

# result = dbm.get_activities("171")
# for r in result:
#     print(r[3])

# dbm.add_activity_label("175")
# dbm.add_activity_labels()

## Exercises

# Exercise 1: How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).
print("Exercise 1: How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).")

# Execute and print the result of the user query
user_query = """SELECT count(id) FROM user"""
dbm.dbc.cursor.execute(user_query)
user_count = dbm.dbc.cursor.fetchone()[0]
print(f"Number of users: {user_count}")

# Execute and print the result of the activity query
activity_query = """SELECT count(id) FROM activity"""
dbm.dbc.cursor.execute(activity_query)
activity_count = dbm.dbc.cursor.fetchone()[0]
print(f"Number of activities: {activity_count}")

# Execute and print the result of the trackpoint query
trackpoint_query = """SELECT count(id) FROM trackpoint"""
dbm.dbc.cursor.execute(trackpoint_query)
trackpoint_count = dbm.dbc.cursor.fetchone()[0]
print(f"Number of trackpoints: {trackpoint_count}")

# Exercise 2: Find the average number of activities per user.
print("\nExercise 2: Find the average number of activities per user.")

# Execute and print the result of the avg query
avg_query = """
            SELECT AVG(activity_count) AS avg_activities_per_user
            FROM (
                SELECT COUNT(*) AS activity_count
                FROM activity
                GROUP BY user_id
            ) AS user_activities;
            """
dbm.dbc.cursor.execute(avg_query)
avg_result = dbm.dbc.cursor.fetchall()[0][0]
print(f"Average number of activities: {avg_result}")

# Exercise 3: Find the top 20 users with the highest number of activities.
print("\nExercise 3: Find the top 20 users with the highest number of activities.")

# Execute and print the result of the top 20 query
top_20_query = """
SELECT user_id, COUNT(*) AS activity_count
FROM activity
GROUP BY user_id
ORDER BY activity_count DESC
LIMIT 20;"""
dbm.dbc.cursor.execute(top_20_query)
top_20_result = dbm.dbc.cursor.fetchall()
print("Top 20 users with the highest number of activities:")
for r in top_20_result:
    print(f"User: {r[0]} Count: {r[1]}")

# Exercise 4: Find all users who have taken a taxi.
print("\nExercise 4: Find all users who have taken a taxi.")

# Execute and print the result of the taxi query
taxi_query = """SELECT DISTINCT user_id, transportation_mode FROM `activity` WHERE transportation_mode="taxi"
"""
dbm.dbc.cursor.execute(taxi_query)
taxi_result = dbm.dbc.cursor.fetchall()
print("Users who have taken a taxi:")
for r in taxi_result:
    print(f"User: {r[0]}")

# Exercise 5: Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. Do not count the rows where the mode is null.
print("\nExercise 5: Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. Do not count the rows where the mode is null.")

# Execute and print the result of the transportation mode query
type_tr_query = """
SELECT transportation_mode, COUNT(*) AS activity_count
FROM activity
WHERE transportation_mode !=""
GROUP BY transportation_mode;
"""
dbm.dbc.cursor.execute(type_tr_query)
type_tr_result = dbm.dbc.cursor.fetchall()
print("Transportation modes and their activity counts:")
for r in type_tr_result:
    print(f"Mode: {r[0]} Count: {r[1]}")

# Exercise 6:
print("\nExercise 6:")
# a) Find the year with the most activities.
print("a) Find the year with the most activities.")

# Execute and print the result of the year query
e6a_query = """
SELECT YEAR(start_date_time) AS year, COUNT(*) AS activity_count
FROM activity
GROUP BY YEAR(start_date_time)
ORDER BY activity_count DESC
LIMIT 1;
"""
dbm.dbc.cursor.execute(e6a_query)
e6a_result = dbm.dbc.cursor.fetchone()
print(f"Year: {e6a_result[0]} Number of activities: {e6a_result[1]}")

# b) Is this also the year with most recorded hours?
print("b) Is this also the year with most recorded hours?")

# Execute and print the result of the hours query
e6b_query = """
SELECT YEAR(start_date_time) AS year, SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) AS total_hours
FROM activity
GROUP BY YEAR(start_date_time)
ORDER BY total_hours DESC
LIMIT 1;
"""
dbm.dbc.cursor.execute(e6b_query)
e6b_result = dbm.dbc.cursor.fetchone()
print(f"Year: {e6b_result[0]} Total hours: {e6b_result[1]}")

# Exercise 8: Find the total distance (in km) walked in 2008, by user with id=112.
print("\nExercise 8: Find the total distance (in km) walked in 2008, by user with id=112.")

# Fetch the activities for user 112 where the transportation_mode is 'walk' and the year is 2008
activity_query = """
    SELECT id
    FROM activity
    WHERE user_id = %s AND transportation_mode = 'walk'
    AND YEAR(start_date_time) = 2008;
    """
dbm.dbc.cursor.execute(activity_query, ("112",))
activity_ids = [row[0] for row in dbm.dbc.cursor.fetchall()]

total_distance = 0.0

# Define the trackpoint query outside the loop
track_query = """
    SELECT latitude, longitude
    FROM trackpoint
    WHERE activity_id = %s
    ORDER BY date_time ASC;
    """

# Fetch corresponding track points and calculate the total distance
for activity_id in activity_ids:
    dbm.dbc.cursor.execute(track_query, (activity_id,))
    track_points = dbm.dbc.cursor.fetchall()

    # Calculate distance between consecutive track points
    for i in range(len(track_points) - 1):
        lat1, lon1 = track_points[i]
        lat2, lon2 = track_points[i + 1]
        total_distance += haversine((lat1, lon1), (lat2, lon2), unit=Unit.KILOMETERS)

print(f"Total distance walked in 2008 by user 112: {total_distance:.2f} km")




# Exercise 9: Find the top 20 users who have gained the most altitude meters.
print("\nExercise 9: Find the top 20 users who have gained the most altitude meters.")

# Fetch all user IDs
user_query = """SELECT DISTINCT user_id FROM activity"""
dbm.dbc.cursor.execute(user_query)
user_ids = [row[0] for row in dbm.dbc.cursor.fetchall()]

from collections import defaultdict

# Process each user iteratively
altitude_gains = defaultdict(float)

track_query = """
    SELECT tp.activity_id, tp.altitute, tp.date_time
    FROM trackpoint tp
    JOIN activity a ON tp.activity_id = a.id
    WHERE a.user_id = %s
    ORDER BY tp.activity_id, tp.date_time;
"""
for user_id in user_ids:
    # print(f"Processing user {user_id}...")

    # Fetch relevant trackpoints for the user
    dbm.dbc.cursor.execute(track_query, (user_id,))
    trackpoints = dbm.dbc.cursor.fetchall()

    previous_altitude = None

    for activity_id, altitude, date_time in trackpoints:
        if previous_altitude is not None and altitude > previous_altitude:
            altitude_gains[user_id] += altitude - previous_altitude
        previous_altitude = altitude

# Sort and get the top 20 users by altitude gain
top_20_altitude_gains = sorted(altitude_gains.items(), key=lambda x: x[1], reverse=True)[:20]

print("Top 20 users with the most altitude meters gained:")
for user_id, total_gain in top_20_altitude_gains:
    print(f"User: {user_id} Total meters gained: {total_gain}")
