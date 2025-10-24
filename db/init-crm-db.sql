CREATE TABLE clients (
    id SERIAL PRIMARY KEY,
    client_id VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    gender VARCHAR(10)
);

INSERT INTO clients (client_id, first_name, last_name, email, gender) VALUES
('CLI001', 'User', 'One', 'user1@example.com', 'Male'),
('CLI002', 'User', 'Two', 'user2@example.com', 'Female'),
('CLI003', 'Prothetic', 'One', 'prothetic1@example.com', 'Male'),
('CLI004', 'Prothetic', 'Two', 'prothetic2@example.com', 'Female'),
('CLI005', 'Prothetic', 'Three', 'prothetic3@example.com', 'Male');