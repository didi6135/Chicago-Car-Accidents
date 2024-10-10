from flask import Flask
from controller.accidents_controller import accidents_controller
from controller.initialize_database import initialize_database
from controller.statistics_controller import injury_statistics_controller



app = Flask(__name__)

app.register_blueprint(blueprint=accidents_controller, url_prefix='/api')
app.register_blueprint(blueprint=injury_statistics_controller, url_prefix='/api')
app.register_blueprint(blueprint=initialize_database, url_prefix='/api')



if __name__ == '__main__':
    # insert_aggregated_data()
    # load_accident_data_service()
    app.run(debug=True)