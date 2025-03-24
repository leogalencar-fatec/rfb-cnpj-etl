-- Enable local file import globally
SET GLOBAL local_infile = 1;

-- Set charset to UTF-8
SET NAMES utf8mb4;
SET CHARACTER SET utf8mb4;

-- Check and set secure_file_priv (optional)
SHOW VARIABLES LIKE 'secure_file_priv';

-- Disable SQL strict mode temporary (mandatory)
SET GLOBAL sql_mode = '';
SET SESSION sql_mode = '';

-- Create database if not exists (change 'my_database' to correspond src/config/config.yaml)
CREATE DATABASE IF NOT EXISTS my_database CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Grant necessary permissions (change 'my_user' to correspond src/config/config.yaml)
GRANT FILE ON *.* TO 'my_user'@'%';
GRANT ALL PRIVILEGES ON my_database.* TO 'my_user'@'%';

-- Flush privileges
FLUSH PRIVILEGES;
