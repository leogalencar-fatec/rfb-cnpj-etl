from email.policy import default
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
            input("Select only regular 'Empresas'? (y/N): ").strip().lower() == "y"
        )
        save_config(config)

        exitStatus = 0
        steps = ["EXTRACT", "TRANSFORM", "LOAD"]

        while not exitStatus:
            for i, step in enumerate(steps):
                print(f"{i + 1}. {step}")
            print("0. Exit")
            steps_to_exec = int(input("Step to execute: "))

            match (steps_to_exec):
                case 1:
                    extract.extract_data()
                    print("Extracted data successfully.")
                case 2:
                    transform.transform_data()
                    print("Transformed data successfully.")
                case 3:
                    load.load_data()
                    print("Loaded data successfully.")
                case _:
                    exitStatus = 1
                    break
    else:
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
