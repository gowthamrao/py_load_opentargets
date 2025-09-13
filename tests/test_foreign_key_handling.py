import pytest
from testcontainers.postgres import PostgresContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
import psycopg

from py_load_opentargets.backends.postgres import PostgresLoader


@pytest.fixture(scope="module")
def postgres_container():
    """Starts a PostgreSQL container for the test module."""
    with PostgresContainer("postgres:16").waiting_for(
        LogMessageWaitStrategy("database system is ready to accept connections")
    ) as container:
        yield container


@pytest.fixture
def db_loader(postgres_container: PostgresContainer):
    """Provides a connected PostgresLoader instance for each test."""
    conn_str = postgres_container.get_connection_url().replace("+psycopg2", "")
    loader = PostgresLoader()
    loader.connect(conn_str)

    # Setup initial schema
    setup_sql = """
    CREATE TABLE public.parent (
        id INT PRIMARY KEY
    );
    CREATE TABLE public.child (
        id INT PRIMARY KEY,
        parent_id INT
    );
    INSERT INTO public.parent (id) VALUES (1), (2);
    INSERT INTO public.child (id, parent_id) VALUES (10, 1), (20, 2);

    ALTER TABLE public.child
    ADD CONSTRAINT fk_child_parent
    FOREIGN KEY (parent_id) REFERENCES public.parent(id);
    """
    loader.cursor.execute(setup_sql)
    loader.conn.commit()

    yield loader

    # Teardown
    loader.cursor.execute(
        "DROP TABLE IF EXISTS public.child; DROP TABLE IF EXISTS public.parent;"
    )
    loader.conn.commit()
    loader.cleanup()


def test_get_foreign_keys(db_loader: PostgresLoader):
    """Tests that foreign keys are correctly identified."""
    fks = db_loader.get_foreign_keys("public.child")
    assert len(fks) == 1
    fk = fks[0]
    assert fk["name"] == "fk_child_parent"
    # DDL from pg_get_constraintdef might not be schema-qualified, so test is less strict.
    assert "FOREIGN KEY (parent_id) REFERENCES parent(id)" in fk["ddl"]


def test_drop_and_recreate_foreign_keys(db_loader: PostgresLoader):
    """
    Tests the full drop and recreate cycle of foreign keys.
    """
    table_name = "public.child"

    # 1. Get the foreign keys
    fks = db_loader.get_foreign_keys(table_name)
    assert len(fks) == 1

    # 2. Drop the foreign keys
    db_loader.drop_foreign_keys(table_name, fks)

    # 3. Verify FK is dropped by inserting an orphan row (should succeed)
    try:
        db_loader.cursor.execute(
            "INSERT INTO public.child (id, parent_id) VALUES (30, 99);"
        )
        db_loader.conn.commit()
    except psycopg.errors.ForeignKeyViolation:
        pytest.fail(
            "ForeignKeyViolation was raised, but constraint should have been dropped."
        )

    # 4. Clean up the orphan row before recreating the constraint
    db_loader.cursor.execute("DELETE FROM public.child WHERE id = 30;")
    db_loader.conn.commit()

    # 5. Recreate the foreign keys
    db_loader.recreate_foreign_keys(table_name, fks)

    # 6. Verify FK is recreated by inserting another orphan row (should fail)
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        db_loader.cursor.execute(
            "INSERT INTO public.child (id, parent_id) VALUES (40, 101);"
        )
        db_loader.conn.commit()

    # The transaction is now in a failed state, so we must roll it back
    # before the fixture can clean up the tables.
    db_loader.conn.rollback()


def test_get_foreign_keys_no_fks(db_loader: PostgresLoader):
    """Tests that an empty list is returned for a table with no FKs."""
    fks = db_loader.get_foreign_keys("public.parent")
    assert len(fks) == 0
