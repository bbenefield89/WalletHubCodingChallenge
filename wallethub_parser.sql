-- create database
CREATE DATABASE wallethub_parser;

-- switch to correct database
USE wallethub_parser;

-- create log_activity table
CREATE TABLE log_activity (
  id INT AUTO_INCREMENT PRIMARY KEY,
  ip_address VARCHAR(255) NOT NULL,
  request_type VARCHAR(255) NOT NULL,
  http_status_code INT NOT NULL,
  useragent TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT NOW()
);
