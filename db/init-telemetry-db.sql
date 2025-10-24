CREATE TABLE telemetry (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    client_id VARCHAR(50) NOT NULL,
    prosthesis_type VARCHAR(50) NOT NULL,
    joint_angle DECIMAL(5,2) NOT NULL,
    pressure_sensor DECIMAL(6,2) NOT NULL,
    emg_signal DECIMAL(8,4) NOT NULL,
    battery_level DECIMAL(5,2) NOT NULL,
    temperature DECIMAL(4,2) NOT NULL,
    activity_type VARCHAR(50) NOT NULL
);

INSERT INTO telemetry (timestamp, client_id, prosthesis_type, joint_angle, pressure_sensor, emg_signal, battery_level, temperature, activity_type)
SELECT 
    NOW() - (random() * INTERVAL '30 days') as timestamp,
    client_id,
    prosthesis_type,
    joint_angle,
    pressure_sensor,
    emg_signal,
    battery_level,
    temperature,
    activity_type
FROM (
    SELECT 
        (ARRAY['CLI001', 'CLI002', 'CLI003', 'CLI004', 'CLI005'])[floor(random() * 5 + 1)] as client_id,
        (ARRAY['Transradial', 'Transhumeral', 'Transtibial', 'Transfemoral'])[floor(random() * 4 + 1)] as prosthesis_type,
        (random() * 180)::DECIMAL(5,2) as joint_angle,
        (random() * 1000)::DECIMAL(6,2) as pressure_sensor,
        (random() * 2 - 1)::DECIMAL(8,4) as emg_signal,
        (random() * 100)::DECIMAL(5,2) as battery_level,
        (20 + random() * 15)::DECIMAL(4,2) as temperature,
        (ARRAY['Walking', 'Running', 'Standing', 'Sitting', 'Climbing', 'Descending'])[floor(random() * 6 + 1)] as activity_type
    FROM generate_series(1, 300)
) as telemetry_data;