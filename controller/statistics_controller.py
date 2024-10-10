# controller/injury_statistics_controller.py

from flask import Blueprint, jsonify, request

from repository.statistics_repository import get_injury_statistics_by_area

# Change name to something unique
injury_statistics_controller = Blueprint('injury_statistics_controller', __name__)


@injury_statistics_controller.route('/injury_statistics', methods=['GET'])
def injury_statistics():
    area = request.args.get('area')

    if not area:
        return jsonify({"error": "Missing required parameter: area"}), 400

    result = get_injury_statistics_by_area(area)

    if result:
        return jsonify(result), 200
    else:
        return jsonify({"error": "No injury statistics found for the specified area"}), 404
