# ETL Project for Brazilian Government Data

## Overview
This ETL (Extract, Transform, Load) project is designed to extract enterprise data from the Brazilian government official site, process it, and load it into a database. The project is structured to facilitate easy maintenance and scalability.

## Project Structure
```
rfb-cnpj-etl
├── .gitignore
├── LICENSE
├── README.md
├── requirements.txt
├── config
│   └── config.yaml
├── data
├── docs
├── logs
├── src
│   ├── extract
│   │   └── extract_data.py
│   ├── load
│   │   └── load_data.py
│   ├── transform
│   │   └── transform_data.py
│   ├── utils
│   │   └── helpers.py
│   └── main.py
└── tests
```

## Components
- **Extract**: The `extract_data.py` file contains functions to connect to the Brazilian government official site, download the enterprise data, and extract it from a zip file.
- **Transform**: The `transform_data.py` file includes functions to process and clean the extracted data.
- **Load**: The `load_data.py` file is responsible for loading the transformed data into a database.
- **Utils**: The `helpers.py` file contains utility functions used across the ETL process.
- **Main**: The `main.py` file serves as the entry point for the ETL process.

## Configuration
The `config.yaml` file contains configuration settings for the ETL process, including database connection details and URLs for the data source.

## Requirements
The `requirements.txt` file lists the Python dependencies required for the project, such as:
- `requests`: For making HTTP requests.
- `pandas`: For data manipulation.
- `SQLAlchemy`: For database interactions.

## Setup Instructions
1. Clone the repository.
2. Navigate to the project directory.
3. Install the required dependencies using:
   ```
   pip install -r requirements.txt
   ```
4. Update the `config.yaml` file with your database connection details and data source URLs.
5. Run the ETL process by executing:
   ```
   python src/main.py
   ```

## Usage Examples
- To extract data, call the `extract_data()` function from the `extract_data.py` module.
- To transform the data, use the `transform_data(data)` function from the `transform_data.py` module.
- To load the data into the database, invoke the `load_data(transformed_data)` function from the `load_data.py` module.

## License
This project is licensed under the MIT License.