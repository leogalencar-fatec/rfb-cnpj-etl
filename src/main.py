import extract.extract_data as extract
import transform.transform_data as transform
import load.load_data as load
import logging

from utils.helpers import load_config, save_config


# Configuration
config = load_config()
initial_config = load_config()
ASK_USER = config["settings"]["ask_user"]


def main():
    # Logging setup
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # User interaction definition
    if ASK_USER:
        config["settings"]["estabelecimentos_apta_only"] = (
            input("Deseja somente as empresas aptas? (y/N): ").strip().lower() == "y"
        )
        save_config(config)

    # Step 1: Extract data
    raw_data = extract.extract_data()

    # # Step 2: Transform data
    transformed_data = transform.transform_data(raw_data)

    # Step 3: Load data into the database
    load.load_data(transformed_data)
    
    # Return config to initial state
    save_config(initial_config)


if __name__ == "__main__":
    main()
