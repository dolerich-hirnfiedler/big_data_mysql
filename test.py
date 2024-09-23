import DbConnector as db
# import example as ex
from DBManager import DBManager as dbm

db=db.DbConnector("localhost", "testdb", "testuser",PASSWORD="test123")

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
dbm.add_activity_labels()
