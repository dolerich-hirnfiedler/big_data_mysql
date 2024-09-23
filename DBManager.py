#!/usr/bin/env python3
import os
from DbConnector import DbConnector as dbc
import mysql.connector as mysql
import csv
from datetime import datetime


class DBManager:
    def __init__(self, dbc: dbc):
        self.dbc = dbc

        self.user_ids = [name for name in os.listdir("./dataset/Data/")]

        # init self.lines
        file_path = "./dataset/labeled_ids.txt"
        # Initialize an empty list to store the lines
        self.user_with_labels = []
        # Open the file and read it
        with open(file_path, "r") as file:
            # Read each line in the file, strip any trailing newline characters, and add to the list
            # lines = file.readlines()
            self.user_with_labels = [line.strip() for line in file]
        # Alternatively, to remove newline characters:
        # Print the list to see the output
        # print(lines)

    def init_Users(self):
        query = """CREATE TABLE IF NOT EXISTS user (
                   id VARCHAR(3) NOT NULL PRIMARY KEY,
                   has_labels boolean)
                """
        # This adds table_name to the %s variable and executes the query
        self.dbc.cursor.execute(query)
        self.dbc.db_connection.commit()

    def init_Activity(self):
        query = """CREATE TABLE IF NOT EXISTS activity (
                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    user_id VARCHAR(3),
                    transportation_mode VARCHAR(30),
                    start_date_time DATETIME,
                    end_date_time DATETIME,
                    FOREIGN KEY (user_id) REFERENCES user(id) ON DELETE CASCADE)
                """
        # This adds table_name to the %s variable and executes the query
        self.dbc.cursor.execute(query)
        self.dbc.db_connection.commit()

    def init_TrackPoint(self):
        query = """CREATE TABLE IF NOT EXISTS trackpoint (
                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    activity_id INT,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    altitute INT,
                    date_days DOUBLE,
                    date_time DATETIME,
                    FOREIGN KEY (activity_id) REFERENCES activity(id) ON DELETE CASCADE )
                """
        # This adds table_name to the %s variable and executes the query
        self.dbc.cursor.execute(query)
        self.dbc.db_connection.commit()

    def init_tables(self):
        self.init_Users()
        self.init_Activity()
        self.init_TrackPoint()

    def insert_user(self, user_id, has_labels):
        try:
            query = "INSERT INTO user (id, has_labels) VALUES (%s, %s)"
            self.dbc.cursor.execute(query, (user_id, has_labels))
            self.dbc.db_connection.commit()
            print(f"User {user_id} inserted successfully")
        except mysql.Error as err:
            print(f"Error inserting user {user_id}: {err}")

    def create_users(self):
        for name in self.user_ids:
            # check if name is in lines
            # if names is in lines insert_user with has_label true,
            # otherwise insert_user with has_label false
            if name in self.user_with_labels:
                self.insert_user(name, True)
            else:
                self.insert_user(name, False)

    def insert_activity(self, user_id, start_date, end_date):
        try:
            query = """INSERT INTO activity (user_id, transportation_mode ,start_date_time,end_date_time) VALUES(%s,%s ,%s,%s)"""
            self.dbc.cursor.execute(query, (user_id, "", start_date, end_date))
            self.dbc.db_connection.commit()
            print(f"Activity inserted sucessfully")
        except mysql.Error as err:
            print(f"Error inserting activity: {err}")

    def create_activities(self):
        for user_id in self.user_ids:
            user_dir = "./dataset/Data/" + user_id + "/Trajectory/"
            activities = [name for name in os.listdir(user_dir)]
            for activity in activities:
                activity_path = user_dir + activity
                # print(activity_path)
                with open(activity_path, "r") as fc:
                    content = fc.readlines()
                    x = len(content)

                    # we are ignoring the first 6 lines while parsing
                    if x <= 2506:
                        first_line = content[6].split(",")
                        last_line = content[x - 1].split(",")
                        first_date = first_line[5] + " " + first_line[6]
                        last_date = last_line[5] + " " + last_line[6]
                        # print(f"First_Date: {first_date}\nLast_Date: {last_date}")
                        self.insert_activity(user_id, first_date, last_date)

    def get_activities(self, user_id):
        query = f"SELECT * FROM activity WHERE user_id = {user_id}"
        self.dbc.cursor.execute(query)
        return self.dbc.cursor.fetchall()

    def get_labels(self, user_id):
        labels_path = f"./dataset/Data/{user_id}/labels.txt"
        labels = []
        with open(labels_path, "r") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                # Convert the start and end time to the desired format
                start_time = format_datetime(row["Start Time"])
                end_time = format_datetime(row["End Time"])

                # Append each row to the list as a dictionary
                labels.append(
                    {
                        "start_time": start_time,
                        "end_time": end_time,
                        "transportation_mode": row["Transportation Mode"],
                    }
                )
        return labels

    def add_activity_label(self, user_id):
        query = f"UPDATE activity SET transportation_mode= %s WHERE id= %s"
        database_activities = self.get_activities(user_id)
        file_labels = self.get_labels(user_id)
        for entry in file_labels:
            file_start_time = entry["start_time"]
            file_end_time = entry["end_time"]
            for db_activity in database_activities:
                db_start_time = db_activity[3].strftime("%Y-%m-%d %H:%M:%S")
                db_end_time = db_activity[4].strftime("%Y-%m-%d %H:%M:%S")
                # print(f"{file_start_time} {db_start_time}")
                # print(f"{type(file_start_time)} {type(db_start_time)}")
                value = (
                    db_start_time == file_start_time and db_end_time == file_end_time
                )
                # print(value)
                if value:
                    # TODO Update activities entry with transportation_mode
                    activity_id = str(db_activity[0])
                    self.dbc.cursor.execute(
                        query, (entry["transportation_mode"], activity_id)
                    )
                    self.dbc.db_connection.commit()
                    print(f"updated value")
                    break

    def add_activity_labels(self):
        for user_id in self.user_with_labels:
            self.add_activity_label(user_id)



def format_datetime(date_string):
    # Parse the date string in the format 'YYYY/MM/DD HH:MM:SS'
    date_obj = datetime.strptime(date_string, "%Y/%m/%d %H:%M:%S")
    # Return the date in the format 'YYYY-MM-DD HH:MM:SS'
    return date_obj.strftime("%Y-%m-%d %H:%M:%S")
