
# importing module
from pymongo import MongoClient

# creation of MongoClient
client=MongoClient()

# Connect with the portnumber and host
client = MongoClient("mongodb://172.27.78.83:27018/", fsync=False, j=False)

# Access database
mydatabase = client["aaron"]

# Access collection of the database
mycollection=mydatabase["aaron"]

# dictionary to be added in the database
record={
"title": 'MongoDB and Python Aaron',
"description": 'MongoDB is no SQL database',
"viewers": 104
}

# inserting the data in the database
rec = mydatabase.aaron.insert(record)
