from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query
from sqlalchemy import create_engine, text

from bdi_api.settings import Settings

import os

settings = Settings()

s5 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s5",
    tags=["s5"],
)


@s5.post("/db/init")
def init_database() -> str:
    """Create all HR database tables (department, employee, project,
    employee_project, salary_history) with their relationships and indexes.

    Use the BDI_DB_URL environment variable to configure the database connection.
    Default: sqlite:///hr_database.db
    """
    # TODO: Connect to the database using SQLAlchemy or psycopg2
    # TODO: Execute the schema creation SQL (see hr_schema.sql)


# 1. Create the connection
    engine = create_engine(settings.db_url)
    
    # 2. Use dynamic pathing to find the file you copied into the folder
    current_dir = os.path.dirname(__file__)
    sql_path = os.path.join(current_dir, "hr_schema.sql")
    
    # 3. Read the SQL file
    with open(sql_path, "r") as f:
        sql_commands = f.read()
    
    # 4. Execute the SQL commands
    with engine.connect() as connection:
        connection.execute(text(sql_commands))
        connection.commit()
        
    return "OK"


@s5.post("/db/seed")
def seed_database() -> str:
    """Populate the HR database with sample data.

    Inserts departments, employees, projects, assignments, and salary history.
    """
    # TODO: Connect to the database
    # TODO: Execute the seed data SQL (see hr_seed_data.sql)

    engine = create_engine(settings.db_url)
    
    # This logic is already perfect!
    current_dir = os.path.dirname(__file__)
    sql_path = os.path.join(current_dir, "hr_seed_data.sql")
    
    with open(sql_path, "r") as f:
        sql_content = f.read()
    
    with engine.connect() as conn:
        conn.execute(text(sql_content))
        conn.commit()
        
    return "OK"


@s5.get("/departments/")
def list_departments() -> list[dict]:
    """Return all departments.

    Each department should include: id, name, location
    """
    # TODO: Query all departments and return as list of dicts
    # 1. Initialize the engine using your environment variable
    engine = create_engine(settings.db_url) 
    
    # 2. Execute the SELECT query
    query = text("SELECT id, name, location FROM department")
    
    with engine.connect() as conn:
        result = conn.execute(query)
        # 3. Convert rows to a list of dictionaries for FastAPI JSON response
        # Using ._mapping is the cleanest way to convert SQLAlchemy rows to dicts
        departments = [dict(row._mapping) for row in result]
    
    return departments


@s5.get("/employees/")
def list_employees(
    page: Annotated[
        int,
        Query(description="Page number (1-indexed)", ge=1),
    ] = 1,
    per_page: Annotated[
        int,
        Query(description="Number of employees per page", ge=1, le=100),
    ] = 10,
) -> list[dict]:
    """Return employees with their department name, paginated.

    Each employee should include: id, first_name, last_name, email, salary, department_name
    """
    # TODO: Query employees with JOIN to department, apply OFFSET and LIMIT
    engine = create_engine(settings.db_url)
    
    # Calculate how many rows to skip
    offset_value = (page - 1) * per_page
    
    # SQL Query with JOIN and Pagination
    query = text("""
        SELECT 
            e.id, 
            e.first_name, 
            e.last_name, 
            e.email, 
            e.salary, 
            d.name AS department_name
        FROM employee e
        JOIN department d ON e.department_id = d.id
        ORDER BY e.id
        LIMIT :limit OFFSET :offset
    """)
    
    with engine.connect() as conn:
        result = conn.execute(
            query, 
            {"limit": per_page, "offset": offset_value}
        )
        # Convert the result rows into a list of dictionaries
    return [dict(row._mapping) for row in result]


@s5.get("/departments/{dept_id}/employees")
def list_department_employees(dept_id: int) -> list[dict]:
    """Return all employees in a specific department.

    Each employee should include: id, first_name, last_name, email, salary, hire_date
    """
    # TODO: Query employees filtered by department_id
    engine = create_engine(settings.db_url)
    
    # We filter by the department_id passed in the URL
    query = text("""
        SELECT id, first_name, last_name, email, salary, hire_date
        FROM employee
        WHERE department_id = :dept_id
        ORDER BY last_name ASC
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"dept_id": dept_id})
        return [dict(row._mapping) for row in result]


@s5.get("/departments/{dept_id}/stats")
def department_stats(dept_id: int) -> dict:
    """Return KPI statistics for a department.

    Response should include: department_name, employee_count, avg_salary, project_count
    """
    # TODO: Calculate department statistics using JOINs and aggregations
    engine = create_engine(settings.db_url)
    
    # We use LEFT JOINs so that if a department has 0 employees 
    # or 0 projects, it still shows up in the results.
    query = text("""
        SELECT 
            d.name AS department_name,
            COUNT(DISTINCT e.id) AS employee_count,
            AVG(e.salary) AS avg_salary,
            COUNT(DISTINCT p.id) AS project_count
        FROM department d
        LEFT JOIN employee e ON d.id = e.department_id
        LEFT JOIN project p ON d.id = p.department_id
        WHERE d.id = :dept_id
        GROUP BY d.id, d.name
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"dept_id": dept_id}).mappings().first()
        
        if not result:
            return {"error": "Department not found"}
            
        # Convert to a standard dict and handle potential None values
        return {
            "department_name": result["department_name"],
            "employee_count": result["employee_count"],
            "avg_salary": float(result["avg_salary"]) if result["avg_salary"] else 0,
            "project_count": result["project_count"]
        }
    return {}


@s5.get("/employees/{emp_id}/salary-history")
def salary_history(emp_id: int) -> list[dict]:
    """Return the salary evolution for an employee, ordered by date.

    Each entry should include: change_date, old_salary, new_salary, reason
    """
    # TODO: Query salary_history for the given employee, ordered by change_date
    engine = create_engine(settings.db_url)
    
    # We select the specific columns requested and order them chronologically
    query = text("""
        SELECT change_date, old_salary, new_salary, reason
        FROM salary_history
        WHERE employee_id = :emp_id
        ORDER BY change_date ASC
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"emp_id": emp_id})
        return [dict(row._mapping) for row in result]
