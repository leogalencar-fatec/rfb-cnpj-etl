-- Enable local file import globally
SET GLOBAL local_infile = 1;

-- Check and set secure_file_priv (optional)
SHOW VARIABLES LIKE 'secure_file_priv';

-- Create database if not exists (change 'my_database' to correspond src/config/config.yaml)
CREATE DATABASE IF NOT EXISTS brazil_cnpj CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Grant necessary permissions (change 'my_user' to correspond src/config/config.yaml)
GRANT FILE ON *.* TO 'root'@'%';
GRANT ALL PRIVILEGES ON my_database.* TO 'root'@'%';

-- Flush privileges
FLUSH PRIVILEGES;
