from flask import Blueprint, request, jsonify
from service.accident_service import get_accidents_by_area_time_period
from datetime import datetime

accidents_controller = Blueprint('/accident', __name__)

@accidents_controller.route('/accidents_by_area_time_period', methods=['GET'])
def accidents_by_area_time_period():
    area = request.args.get('area')
    period_type = request.args.get('period_type')
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')

    # Validate inputs
    if not area or not period_type or not start_date_str or not end_date_str:
        return jsonify({"error": "Missing required query parameters"}), 400

    # Parse dates
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

    # Call the service layer to get the total accidents
    try:
        total_accidents = get_accidents_by_area_time_period(area, period_type, start_date, end_date)
        return jsonify({
            "area": area,
            "period_type": period_type,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "total_accidents": total_accidents
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500