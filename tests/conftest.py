import pytest
import psycopg
from testcontainers.postgres import PostgresContainer

@pytest.fixture(scope="session")
def db_conn_str():
    """
    Provides a connection string to a temporary, containerized PostgreSQL instance.
    This fixture is session-scoped, so the container is started once per test session.
    """
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres.get_connection_url().replace("+psycopg2", "")

@pytest.fixture(scope="function")
def db_conn(db_conn_str):
    """
    Provides a live, function-scoped database connection to the containerized
    PostgreSQL instance. A new connection is created for each test function
    to ensure test isolation.
    """
    conn = psycopg.connect(db_conn_str)
    yield conn
    # Ensure the connection is closed after the test function completes.
    conn.close()
