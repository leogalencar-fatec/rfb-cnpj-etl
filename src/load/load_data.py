def load_data(transformed_data):
    import sqlalchemy
    from sqlalchemy import create_engine

    # Load database configuration from config
    DATABASE_URI = "your_database_uri_here"  # Replace with actual database URI

    # Create a database connection
    engine = create_engine(DATABASE_URI)
    
    # Load the transformed data into the database
    with engine.connect() as connection:
        # Assuming transformed_data is a DataFrame
        transformed_data.to_sql('your_table_name_here', con=connection, if_exists='append', index=False)  # Replace with actual table name

    print("Data loaded successfully into the database.")