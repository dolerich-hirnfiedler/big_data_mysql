import DbConnector as db

# import example as ex
from DBManager import DBManager as dbm

db = db.DbConnector("localhost", "testdb", "testuser", PASSWORD="test123")

dbm = dbm(db)

# dbm.init_Users()
# dbm.init_Activity()
# dbm.init_TrackPoint()
# dbm.create_users()

# dbm.create_activities()
# dbm.test_stuff()

# results = dbm.get_labels("010")
# for r  in results:
#     print(r)

# result = dbm.get_activities("171")
# for r  in result:
#     print(r[3])

# dbm.add_activity_label("175")
# dbm.add_activity_labels()


## Exercises

# Exercise 1: How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).


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
print(f"Number of activities: {avg_result}")

# Exercise 3: Find the top 20 users with the highest number of activities.

top_20_query = """
SELECT user_id, COUNT(*) AS activity_count
FROM activity
GROUP BY user_id
ORDER BY activity_count DESC
LIMIT 20;"""
dbm.dbc.cursor.execute(top_20_query)
top_20_result = dbm.dbc.cursor.fetchall()
print(f"Top 20 activities:")
for r in top_20_result:
    print(f"User: {r[0]} Count: {r[1]}")

# Exercise 4: Find all users who have taken a taxi.

taxi_query = """SELECT DISTINCT user_id,transportation_mode FROM `activity` WHERE transportation_mode="taxi"
"""

dbm.dbc.cursor.execute(taxi_query)
taxi_result = dbm.dbc.cursor.fetchall()
print(f"Taxi users")
for r in taxi_result:
    print(f"User: {r[0]}")
