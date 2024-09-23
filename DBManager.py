#!/usr/bin/env python3
import csv
from datetime import datetime
import os
from typing import Any, Tuple

import mysql.connector as mysql
from mysql.connector.types import RowType

from DbConnector import DbConnector


class DBManager:
    def __init__(self, dbc: DbConnector):
        self.dbc: DbConnector = dbc
        self.user_ids: list[str] = [name for name in os.listdir("./dataset/Data/")]

        # init self.lines
        file_path = "./dataset/labeled_ids.txt"
        # Initialize an empty list to store the lines
        self.user_with_labels: list[str] = []
        # Open the file and read it
        with open(file_path, "r") as file:
            # Read each line in the file, strip any trailing newline characters, and add to the list
            self.user_with_labels = [line.strip() for line in file]

    def init_Users(self) -> None:
        query: str = """CREATE TABLE IF NOT EXISTS user (
                   id VARCHAR(3) NOT NULL PRIMARY KEY,
                   has_labels BOOLEAN)
                """
        _ = self.dbc.cursor.execute(query)
        _ = self.dbc.db_connection.commit()

    def init_Activity(self) -> None:
        query: str = """CREATE TABLE IF NOT EXISTS activity (
                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    user_id VARCHAR(3),
                    transportation_mode VARCHAR(30),
                    start_date_time DATETIME,
                    end_date_time DATETIME,
                    FOREIGN KEY (user_id) REFERENCES user(id) ON DELETE CASCADE)
                """
        _ = self.dbc.cursor.execute(query)
        _ = self.dbc.db_connection.commit()

    def init_TrackPoint(self) -> None:
        query: str = """CREATE TABLE IF NOT EXISTS trackpoint (
                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    activity_id INT,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    altitute INT,
                    date_days DOUBLE,
                    date_time DATETIME,
                    FOREIGN KEY (activity_id) REFERENCES activity(id) ON DELETE CASCADE)
                """
        _ = self.dbc.cursor.execute(query)
        _ = self.dbc.db_connection.commit()

    def init_tables(self) -> None:
        self.init_Users()
        self.init_Activity()
        self.init_TrackPoint()

    def insert_user(self, user_id: str, has_labels: bool) -> None:
        try:
            query: str = "INSERT INTO user (id, has_labels) VALUES (%s, %s)"
            _ = self.dbc.cursor.execute(query, (user_id, has_labels))
            _ = self.dbc.db_connection.commit()
            print(f"User {user_id} inserted successfully")
        except mysql.Error as err:
            print(f"Error inserting user {user_id}: {err}")

    def create_users(self) -> None:
        for name in self.user_ids:
            if name in self.user_with_labels:
                self.insert_user(name, True)
            else:
                self.insert_user(name, False)

    def insert_activity(self, user_id: str, start_date: str, end_date: str) -> None:
        try:
            query: str = (
                """INSERT INTO activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES(%s,%s,%s,%s)"""
            )
            _ = self.dbc.cursor.execute(query, (user_id, "", start_date, end_date))
            _ = self.dbc.db_connection.commit()
        except mysql.Error as err:
            print(f"Error inserting activity: {err}")

    def create_activities(self) -> None:
        trackpoint_query = "insert into trackpoint (activity_id,latitude, longitude, altitute,date_days,date_time) values (%s,%s,%s,%s,%s,%s)"
        for user_id in self.user_ids:
            user_dir: str = os.path.join("./dataset/Data/", user_id, "Trajectory/")
            activities: list[str] = [name for name in os.listdir(user_dir)]
            for activity in activities:
                activity_path: str = os.path.join(user_dir, activity)
                with open(activity_path, "r") as fc:
                    content: list[str] = fc.readlines()
                    x: int = len(content)

                    # we are ignoring the first 6 lines while parsing
                    if x <= 2506:
                        # This is the part for activities
                        first_line: list[str] = content[6].split(",")
                        last_line: list[str] = content[x - 1].split(",")
                        first_date: str = f"{first_line[5]} {first_line[6]}"
                        last_date: str = f"{last_line[5]} {last_line[6]}"
                        self.insert_activity(user_id, first_date, last_date)
                        activity_id = self.get_activity_id(
                            user_id, first_date, last_date
                        )

                        # generate the trackpoints for the activity
                        trackpoint_tupel: list[tuple[int, str, str, int, str, str]] = (
                            list()
                        )
                        for row in content[6:x]:
                            cells = row.split(",")
                            row_tupel: tuple[int, str, str, int, str, str] = (
                                activity_id,
                                cells[0],
                                cells[1],
                                int(float(cells[3])),
                                cells[4],
                                f"{cells[5]} {cells[6]}",
                            )
                            print(f"{row_tupel}")
                            trackpoint_tupel.append(row_tupel)
                        # query
                        _ = self.dbc.cursor.executemany(
                            trackpoint_query, trackpoint_tupel
                        )
                        _ = self.dbc.db_connection.commit()

    def get_activities(self, user_id: str) -> list[RowType]:
        query: str = "SELECT * FROM activity WHERE user_id = %s"
        _ = self.dbc.cursor.execute(query, (user_id,))
        return self.dbc.cursor.fetchall()

    def get_activity_id(self, user_id: str, first_date: str, last_date: str) -> int:
        query = "SELECT id FROM activity WHERE user_id= %s AND start_date_time = %s AND end_date_time= %s"
        _ = self.dbc.cursor.execute(query, (user_id, first_date, last_date))
        return self.dbc.cursor.fetchone()[0]

    def get_labels(self, user_id: str) -> list[dict[str, str]]:
        labels_path: str = f"./dataset/Data/{user_id}/labels.txt"
        labels: list[dict[str, str]] = []
        with open(labels_path, "r") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                # Convert the start and end time to the desired format
                start_time: str = format_datetime(row["Start Time"])
                end_time: str = format_datetime(row["End Time"])

                # Append each row to the list as a dictionary
                labels.append(
                    {
                        "start_time": start_time,
                        "end_time": end_time,
                        "transportation_mode": row["Transportation Mode"],
                    }
                )
        return labels

    def add_activity_label(self, user_id: str) -> None:
        query: str = "UPDATE activity SET transportation_mode = %s WHERE id = %s"
        database_activities: list[tuple[Any, ...]] = self.get_activities(user_id)
        file_labels: list[dict[str, str]] = self.get_labels(user_id)
        for entry in file_labels:
            file_start_time: str = entry["start_time"]
            file_end_time: str = entry["end_time"]
            for db_activity in database_activities:
                db_start_time: str = db_activity[3].strftime("%Y-%m-%d %H:%M:%S")
                db_end_time: str = db_activity[4].strftime("%Y-%m-%d %H:%M:%S")
                if db_start_time == file_start_time and db_end_time == file_end_time:
                    activity_id: str = str(db_activity[0])
                    _ = self.dbc.cursor.execute(
                        query, (entry["transportation_mode"], activity_id)
                    )
                    _ = self.dbc.db_connection.commit()
                    print("Updated value")
                    break

    def add_activity_labels(self) -> None:
        for user_id in self.user_with_labels:
            self.add_activity_label(user_id)


def format_datetime(date_string: str) -> str:
    # Parse the date string in the format 'YYYY/MM/DD HH:MM:SS'
    date_obj: datetime = datetime.strptime(date_string, "%Y/%m/%d %H:%M:%S")
    # Return the date in the format 'YYYY-MM-DD HH:MM:SS'
    return date_obj.strftime("%Y-%m-%d %H:%M:%S")
