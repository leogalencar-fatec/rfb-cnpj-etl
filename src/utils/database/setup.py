import mysql.connector
from utils.helpers import load_config

config = load_config()
DB_HOST = config["database"]["host"]
DB_USERNAME = config["database"]["username"]
DB_PASSWORD = config["database"]["password"]
DB_NAME = config["database"]["database_name"]
DB_PORT = config["database"]["port"]

def run_mysql_setup():
    """Executes the MySQL setup script."""
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USERNAME,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()

    with open("sql/setup_mysql.sql", "r") as f:
        sql_script = f.read()

    for statement in sql_script.split(";"):
        if statement.strip():
            cursor.execute(statement)

    conn.commit()
    cursor.close()
    conn.close()
    print("MySQL setup script executed.")

def verify_mysql_setup():
    """Verifies if MySQL was properly configured for bulk imports."""
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = conn.cursor()

    # Check if local_infile is enabled
    cursor.execute("SHOW VARIABLES LIKE 'local_infile';")
    local_infile_status = cursor.fetchone()
    
    # Check secure_file_priv value
    cursor.execute("SHOW VARIABLES LIKE 'secure_file_priv';")
    secure_file_priv_status = cursor.fetchone()

    # Check user privileges
    cursor.execute("SHOW GRANTS FOR CURRENT_USER;")
    grants = cursor.fetchall()

    cursor.close()
    conn.close()

    print("\n🔍 **MySQL Configuration Verification**")
    if local_infile_status and local_infile_status[1] == "ON":
        print("`local_infile` is enabled.")
    else:
        print("`local_infile` is NOT enabled. You may need to manually enable it in `my.cnf`.")

    print(f"`secure_file_priv`: {secure_file_priv_status[1]}")

    file_privilege = any("FILE" in grant[0] for grant in grants)
    if file_privilege:
        print("User has `FILE` privilege.")
    else:
        print("User does NOT have `FILE` privilege. Check MySQL permissions.")

    print("\n**MySQL Setup Verification Completed**")


if __name__ == "__main__":
    run_mysql_setup()
    verify_mysql_setup()