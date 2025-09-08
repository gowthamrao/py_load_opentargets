import pytest
import psycopg

@pytest.fixture(scope="session")
def db_conn_str(postgresql_proc):
    """
    Provides a connection string to the temporary, test-managed PostgreSQL instance.
    The `postgresql_proc` fixture is provided by pytest-postgresql and gives access
    to the server details.
    """
    return f"postgresql://{postgresql_proc.user}:{postgresql_proc.password}@{postgresql_proc.host}:{postgresql_proc.port}/{postgresql_proc.dbname}"

@pytest.fixture(scope="function")
def db_conn(postgresql):
    """
    Provides a live database connection to the temporary PostgreSQL instance.
    The `postgresql` fixture is provided by pytest-postgresql and is a
    ready-to-use connection to the auto-created test database.
    """
    yield postgresql
    # The fixture handles closing the connection. The `postgresql` fixture is
    # function-scoped, so this one must be too.
