import datetime
import os
import extract.extract_data as extract
import transform.transform_data as transform
import load.load_data as load
import logging
import copy

from utils.helpers import load_config, save_config

# Configuration
config = load_config()
initial_config = copy.deepcopy(config)  # Store a deep copy of the initial state
ASK_USER = config["settings"]["ask_user"]
LOG_FILE_PATH = config["logging"]["log_path"]

def setup_logging():
    """Configure logging settings for the application."""
    current_datetime = datetime.now().isoformat(timespec='seconds').replace(':', '-')
    log_filename = f"{current_datetime}.log"
    log_filepath = os.path.join(LOG_FILE_PATH, log_filename)
    
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.logging.FileHandler(log_filepath),
            logging.logging.StreamHandler()
        ],
    )

def execute_batch():
    """Run all steps automatically in batch mode."""
    raw_data = extract.extract_data()
    transformed_data = transform.transform_data(raw_data)
    load.load_data(transformed_data)

def execute_interactive():
    """Run steps interactively based on user input."""
    config["settings"]["estabelecimentos_apta_only"] = (
        input("Select only regular 'Empresas'? (y/N): ").strip().lower() == "y"
    )
    save_config(config)

    steps = {
        1: ("Extract", extract.extract_data),
        2: ("Transform", transform.transform_data),
        3: ("Load", load.load_data),
    }

    while True:
        logging.info("\nSelect a step to execute:")
        for num, (name, _) in steps.items():
            logging.info(f"{num}. {name}")
        logging.info("0. Exit")

        try:
            choice = int(input("Step to execute: ").strip())
            if choice == 0:
                break
            elif choice in steps:
                step_name, step_function = steps[choice]
                step_function()
                logging.info(f"{step_name} completed successfully.")
            else:
                logging.warning("Invalid choice. Please select a valid step.")
        except ValueError:
            logging.error("Invalid input. Please enter a number.")

def main():
    """Main function to execute the ETL pipeline."""
    setup_logging()

    if ASK_USER:
        execute_interactive()
    else:
        execute_batch()

    # Restore the initial configuration
    save_config(initial_config)

if __name__ == "__main__":
    main()
