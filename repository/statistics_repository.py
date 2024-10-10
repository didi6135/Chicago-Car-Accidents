from database.connect import get_injury_statistics_by_area_collection


def get_injury_statistics_by_area(area):

    collection = get_injury_statistics_by_area_collection()

    # Query the collection for the specific area
    result = collection.find_one({"area": area})

    if result:
        return {
            "area": result["area"],
            "total_injuries": result["total_injuries"],
            "fatal_injuries": result["fatal_injuries"],
            "non_fatal_injuries": result["non_fatal_injuries"],
            "events": result["events"]
        }
    else:
        return None