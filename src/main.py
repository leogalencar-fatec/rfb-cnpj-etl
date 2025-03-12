import extract.extract_data as extract
import transform.transform_data as transform
import load.load_data as load

def main():
    # Step 1: Extract data
    raw_data = extract.extract_data()
    
    # # Step 2: Transform data
    transformed_data = transform.transform_data(raw_data)
    
    # Step 3: Load data into the database
    load.load_data(transformed_data)

if __name__ == "__main__":
    main()