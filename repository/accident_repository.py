import time
from datetime import datetime, timedelta

from pymongo import ASCENDING

from database.connect import (
    get_accidents_by_day_collection,
    get_accidents_by_week_collection,
    get_accidents_by_month_collection, get_accidents_by_cause_collection, get_accidents_by_area_collection
)

# Get accidents by day from the 'accidents_by_day' collection
# def get_accidents_by_day(area, day):
#     collection = get_accidents_by_day_collection()
#     return collection.find_one({"area": area, "day": day})
def get_accidents_by_day(area, day):
    collection = get_accidents_by_day_collection()
    get_accidents_by_day_collection().create_index([("area", ASCENDING), ("day", ASCENDING)])

    # Query without index (using '$natural' hint to force collection scan)
    no_index_execution_stats = collection.find({"area": area, "day": day}).hint({'$natural': 1}).explain()['executionStats']

    print(f"Query by day without index took {no_index_execution_stats['executionTimeMillis']} ms")
    print(f"Total docs examined without index: {no_index_execution_stats['totalDocsExamined']}")

    # Query with index
    with_index_execution_stats = collection.find({"area": area, "day": day}).hint({'area': 1, 'day': 1}).explain()['executionStats']

    print(f"Query by day with index took {with_index_execution_stats['executionTimeMillis']} ms")
    print(f"Total docs examined with index: {with_index_execution_stats['totalDocsExamined']}")

    # Return actual query result
    return collection.find_one({"area": area, "day": day})


# Get accidents by week from the 'accidents_by_week' collection
def get_accidents_by_week(area, week_start, week_end):
    collection = get_accidents_by_week_collection()

    # Convert the week_start and week_end strings to datetime objects (ignoring time)
    week_start_dt = datetime.strptime(week_start, '%Y-%m-%d')
    week_end_dt = datetime.strptime(week_end, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)  # Include full day

    no_index_execution_stats = collection.find({
        "area": area,
        "week_start": {"$gte": week_start_dt},
        "week_end": {"$lte": week_end_dt}
    }).hint({'$natural': 1}).explain()['executionStats']

    print(f"Query by week without index took {no_index_execution_stats['executionTimeMillis']} ms")
    print(f"Total docs examined without index: {no_index_execution_stats['totalDocsExamined']}")

    # Query with index
    with_index_execution_stats = collection.find({
        "area": area,
        "week_start": {"$gte": week_start_dt},
        "week_end": {"$lte": week_end_dt}
    }).hint({'area': 1, 'week_start': 1, 'week_end': 1}).explain()['executionStats']

    print(f"Query by week with index took {with_index_execution_stats['executionTimeMillis']} ms")
    print(f"Total docs examined with index: {with_index_execution_stats['totalDocsExamined']}")

    # Return actual query result
    return collection.find_one({
        "area": area,
        "week_start": {"$gte": week_start_dt},
        "week_end": {"$lte": week_end_dt}
    })




# # Get accidents by month from the 'accidents_by_month' collection
# def get_accidents_by_month(area, month):
#     collection = get_accidents_by_month_collection()
#     return collection.find_one({"area": area, "month": month})
# Get accidents by month from the 'accidents_by_month' collection
def get_accidents_by_month(area, month):
    collection = get_accidents_by_month_collection()

    # Query without index
    no_index_execution_stats = collection.find({"area": area, "month": month}).hint({'$natural': 1}).explain()['executionStats']
    print(f"Query by month without index took {no_index_execution_stats['executionTimeMillis']} ms")
    print(f"Total docs examined without index: {no_index_execution_stats['totalDocsExamined']}")

    # Query with index
    with_index_execution_stats = collection.find({"area": area, "month": month}).hint({'area': 1, 'month': 1}).explain()['executionStats']

    print(f"Query by month with index took {with_index_execution_stats['executionTimeMillis']} ms")
    print(f"Total docs examined with index: {with_index_execution_stats['totalDocsExamined']}")

    # Return actual query result
    return collection.find_one({"area": area, "month": month})


# Get accidents grouped by the primary cause in a specific area
def get_accidents_grouped_by_cause(area):
    collection = get_accidents_by_cause_collection()

    # Measure query performance without index
    no_index_explain = collection.find({"area": area}).hint({'$natural': 1}).explain()
    print(f"Query without index took {no_index_explain['executionStats']['executionTimeMillis']} ms")


    # Measure query performance with index
    with_index_explain = collection.find({"area": area}).hint({'area': 1}).explain()
    print(f"Query with index took {with_index_explain['executionStats']['executionTimeMillis']} ms")


    # Return the actual query result
    result = collection.find_one({"area": area})
    return result['causes'] if result else None



# def get_accidents_by_area(area):
#     collection = get_accidents_by_area_collection()
#     return list(collection.find({'area': area}).hint({ '$natural': 1}).explain()['executionStats'])

def get_accidents_by_area(area):
    collection = get_accidents_by_area_collection()

    # Query without index (using '$natural' hint to force a collection scan)
    no_index_execution_stats = collection.find({'area': area}).hint({'$natural': 1}).explain()['executionStats']

    # Print execution stats without index
    print(f"Query without index took {no_index_execution_stats['executionTimeMillis']} ms")
    print(f"Total docs examined without index: {no_index_execution_stats['totalDocsExamined']}")

    # Query with index
    with_index_execution_stats = collection.find({'area': area}).hint({'area': 1}).explain()['executionStats']

    # Print execution stats with index
    print(f"Query with index took {with_index_execution_stats['executionTimeMillis']} ms")
    print(f"Total docs examined with index: {with_index_execution_stats['totalDocsExamined']}")

    # Return the actual query result (using index)
    return list(collection.find({'area': area}))






