from datetime import datetime, timedelta

from database.connect import (
    get_accidents_by_day_collection,
    get_accidents_by_week_collection,
    get_accidents_by_month_collection, get_accidents_by_cause_collection, get_accidents_by_area_collection
)

# Get accidents by day from the 'accidents_by_day' collection
def get_accidents_by_day(area, day):
    collection = get_accidents_by_day_collection()
    return collection.find_one({"area": area, "day": day})

# Get accidents by week from the 'accidents_by_week' collection
def get_accidents_by_week(area, week_start, week_end):
    collection = get_accidents_by_week_collection()

    # Convert the week_start and week_end strings to datetime objects (ignoring time)
    week_start_dt = datetime.strptime(week_start, '%Y-%m-%d')
    week_end_dt = datetime.strptime(week_end, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)  # Include full day

    # Find documents where the area matches and week_start and week_end fall within the range
    return collection.find_one({
        "area": area,
        "week_start": {"$gte": week_start_dt},
        "week_end": {"$lte": week_end_dt}
    })

# Get accidents by month from the 'accidents_by_month' collection
def get_accidents_by_month(area, month):
    collection = get_accidents_by_month_collection()
    return collection.find_one({"area": area, "month": month})


# Get accidents grouped by the primary cause in a specific area
def get_accidents_grouped_by_cause(area):
    collection = get_accidents_by_cause_collection()

    # Perform an aggregation to group by cause in the specified area
    pipeline = [
        {"$match": {"area": area}},
        {"$unwind": "$causes"},
        {"$group": {
            "_id": "$causes",
            "total_accidents": {"$sum": 1}
        }},
        {"$sort": {"total_accidents": -1}}
    ]

    result = list(collection.aggregate(pipeline))
    return result


def get_accidents_by_area(area):
    collection = get_accidents_by_area_collection()
    return list(collection.find({'area': area}))
