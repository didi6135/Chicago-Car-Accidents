import os

from flask import Flask, jsonify, request

from controller.accidents_controller import accidents_controller
# from controller.accidents_controller import accidents_controller
from service.csv_service import load_accident_data_service

app = Flask(__name__)

app.register_blueprint(blueprint=accidents_controller, url_prefix='/api')


@app.route('/initialize_db', methods=['POST'])
def initialize_db():
    try:
        load_accident_data_service('data/data.csv')
        return jsonify({"message": "Database initialized successfully."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # insert_aggregated_data()
    # load_accident_data_service()
    app.run(debug=True)