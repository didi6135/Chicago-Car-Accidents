from repository.csv_repository import initialize_database


def load_accident_data_service(csv_file_path):
    initialize_database(csv_file_path)
    print("Accident data loaded and processed successfully.")