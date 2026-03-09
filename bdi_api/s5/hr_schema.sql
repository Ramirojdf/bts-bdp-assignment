DROP TABLE IF EXISTS salary_history CASCADE;
DROP TABLE IF EXISTS employee_project CASCADE;
DROP TABLE IF EXISTS project CASCADE;
DROP TABLE IF EXISTS employee CASCADE;
DROP TABLE IF EXISTS department CASCADE;

CREATE TABLE department (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    location VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE employee (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    hire_date DATE NOT NULL,
    salary DECIMAL(10, 2) NOT NULL,
    department_id INTEGER REFERENCES department(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE project (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    start_date DATE NOT NULL,
    end_date DATE,
    budget DECIMAL(12, 2),
    department_id INTEGER REFERENCES department(id) ON DELETE SET NULL
);

CREATE TABLE employee_project (
    employee_id INTEGER REFERENCES employee(id) ON DELETE CASCADE,
    project_id INTEGER REFERENCES project(id) ON DELETE CASCADE,
    role VARCHAR(50) DEFAULT 'member',
    assigned_date DATE DEFAULT CURRENT_DATE,
    PRIMARY KEY (employee_id, project_id)
);

CREATE TABLE salary_history (
    id SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL REFERENCES employee(id) ON DELETE CASCADE,
    old_salary DECIMAL(10, 2) NOT NULL,
    new_salary DECIMAL(10, 2) NOT NULL,
    change_date DATE NOT NULL DEFAULT CURRENT_DATE,
    reason VARCHAR(200)
);

CREATE INDEX idx_employee_department ON employee(department_id);
CREATE INDEX idx_employee_email ON employee(email);
CREATE INDEX idx_project_department ON project(department_id);
CREATE INDEX idx_salary_history_employee ON salary_history(employee_id);
CREATE INDEX idx_employee_project_employee ON employee_project(employee_id);
CREATE INDEX idx_employee_project_project ON employee_project(project_id);
