# # from pymongo import MongoClient
# #
# # def get_mongo_client():
# #     client = MongoClient('mongodb://172.23.14.159:27017/')  # Change this if necessary
# #     return client
# #
# # def get_accidents_collection():
# #     client = get_mongo_client()
# #     db = client['chicago_car_accidents']
# #     return db['aggregated_accidents']
# from pymongo import MongoClient
#
#
# # Establish MongoDB connection
# def get_mongo_client():
#     client = MongoClient('mongodb://172.23.14.159:27017/')
#     return client
#
# def get_db():
#     client = get_mongo_client()
#     return client['chicago_car_accidents']
#
# # Functions to access different collections
# def get_accidents_by_area_collection():
#     db = get_db()
#     return db['accidents_by_area']
#
# def get_accidents_by_area_time_period_collection():
#     db = get_db()
#     return db['accidents_by_area_time_period']
#
# def get_accidents_by_cause_collection():
#     db = get_db()
#     return db['accidents_by_cause']
#
# def get_injury_statistics_by_area_collection():
#     db = get_db()
#     return db['injury_statistics_by_area']
from pymongo import MongoClient

# Establish MongoDB connection
def get_mongo_client():
    client = MongoClient('mongodb://172.23.14.159:27017/')
    # client = MongoClient('mongodb://localhost:27017/')
    return client

def get_db():
    client = get_mongo_client()
    return client['chicago_car_accidents']

# Functions to access different collections
def get_accidents_by_area_collection(collection_name='accidents_by_area'):
    db = get_db()
    return db[collection_name]


def get_accidents_by_day_collection():
    return get_accidents_by_area_collection('accidents_by_day')


def get_accidents_by_week_collection():
    return get_accidents_by_area_collection('accidents_by_week')


def get_accidents_by_month_collection():
    return get_accidents_by_area_collection('accidents_by_month')


def get_accidents_by_cause_collection():
    db = get_db()
    return db['accidents_by_cause']

def get_injury_statistics_by_area_collection():
    db = get_db()
    return db['injury_statistics_by_area']
