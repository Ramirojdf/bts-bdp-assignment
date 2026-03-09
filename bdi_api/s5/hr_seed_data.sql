INSERT INTO department (name, location) VALUES
    ('Engineering', 'Barcelona'),
    ('Marketing', 'Madrid'),
    ('Human Resources', 'Barcelona'),
    ('Finance', 'London'),
    ('Sales', 'New York');

INSERT INTO employee (first_name, last_name, email, hire_date, salary, department_id) VALUES
    ('Anna', 'Garcia', 'anna.garcia@company.com', '2020-03-15', 55000.00, 1),
    ('Marc', 'Lopez', 'marc.lopez@company.com', '2019-07-01', 62000.00, 1),
    ('Laura', 'Martinez', 'laura.martinez@company.com', '2021-01-10', 48000.00, 2),
    ('Carlos', 'Fernandez', 'carlos.fernandez@company.com', '2018-11-20', 70000.00, 1),
    ('Sofia', 'Rodriguez', 'sofia.rodriguez@company.com', '2022-06-01', 45000.00, 3),
    ('David', 'Sanchez', 'david.sanchez@company.com', '2020-09-15', 58000.00, 4),
    ('Maria', 'Diaz', 'maria.diaz@company.com', '2017-04-01', 75000.00, 4),
    ('Pablo', 'Ruiz', 'pablo.ruiz@company.com', '2023-02-01', 42000.00, 5),
    ('Elena', 'Torres', 'elena.torres@company.com', '2021-08-15', 52000.00, 2),
    ('Jorge', 'Navarro', 'jorge.navarro@company.com', '2019-12-01', 60000.00, 1),
    ('Clara', 'Moreno', 'clara.moreno@company.com', '2022-03-15', 47000.00, 5),
    ('Ivan', 'Jimenez', 'ivan.jimenez@company.com', '2020-05-20', 53000.00, 1);

INSERT INTO project (name, description, start_date, end_date, budget, department_id) VALUES
    ('Data Platform', 'Build the internal data platform', '2024-01-15', '2024-12-31', 150000.00, 1),
    ('Brand Refresh', 'Company rebranding campaign', '2024-03-01', '2024-09-30', 80000.00, 2),
    ('HR Portal', 'Employee self-service portal', '2024-02-01', NULL, 60000.00, 3),
    ('Q4 Budget', 'Annual budget planning', '2024-07-01', '2024-10-31', 25000.00, 4),
    ('Mobile App', 'Customer mobile application', '2024-04-15', NULL, 200000.00, 1),
    ('Sales Dashboard', 'Real-time sales analytics', '2024-05-01', '2024-11-30', 45000.00, 5);

INSERT INTO employee_project (employee_id, project_id, role, assigned_date) VALUES
    (1, 1, 'developer', '2024-01-15'),
    (2, 1, 'lead', '2024-01-15'),
    (4, 1, 'architect', '2024-01-15'),
    (10, 1, 'developer', '2024-02-01'),
    (12, 1, 'developer', '2024-03-01'),
    (3, 2, 'coordinator', '2024-03-01'),
    (9, 2, 'designer', '2024-03-15'),
    (5, 3, 'lead', '2024-02-01'),
    (1, 5, 'developer', '2024-04-15'),
    (2, 5, 'lead', '2024-04-15'),
    (4, 5, 'architect', '2024-04-15'),
    (12, 5, 'developer', '2024-05-01'),
    (6, 4, 'analyst', '2024-07-01'),
    (7, 4, 'lead', '2024-07-01'),
    (8, 6, 'developer', '2024-05-01'),
    (11, 6, 'analyst', '2024-05-15');

INSERT INTO salary_history (employee_id, old_salary, new_salary, change_date, reason) VALUES
    (1, 48000.00, 52000.00, '2021-03-15', 'Annual review'),
    (1, 52000.00, 55000.00, '2023-03-15', 'Promotion'),
    (2, 55000.00, 58000.00, '2020-07-01', 'Annual review'),
    (2, 58000.00, 62000.00, '2022-07-01', 'Promotion to lead'),
    (4, 60000.00, 65000.00, '2020-11-20', 'Annual review'),
    (4, 65000.00, 70000.00, '2022-11-20', 'Promotion to architect'),
    (7, 65000.00, 70000.00, '2019-04-01', 'Annual review'),
    (7, 70000.00, 75000.00, '2021-04-01', 'Promotion'),
    (6, 50000.00, 54000.00, '2021-09-15', 'Annual review'),
    (6, 54000.00, 58000.00, '2023-09-15', 'Annual review'),
    (10, 52000.00, 56000.00, '2021-12-01', 'Annual review'),
    (10, 56000.00, 60000.00, '2023-12-01', 'Promotion');
