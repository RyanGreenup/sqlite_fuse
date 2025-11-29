#!/usr/bin/env python3
"""
Build a SQLite database with specific created_at and updated_at timestamps
for testing FUSE filesystem ctime and mtime correctness.

Loads schema from sql/init.sql and populates with test data.
"""

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import typer
import uuid

app = typer.Typer(help="Build test SQLite database with specific timestamps")

# Colors for terminal output
GREEN = "\033[92m"
BLUE = "\033[94m"
RESET = "\033[0m"


def find_sql_init_file() -> Path:
    """Find sql/init.sql relative to this script"""
    script_dir = Path(__file__).parent
    sql_file = script_dir / "sql" / "init.sql"

    if not sql_file.exists():
        raise FileNotFoundError(f"Schema file not found at {sql_file}")

    return sql_file


def init_schema(db_path: Path) -> None:
    """Initialize database schema from sql/init.sql"""
    sql_file = find_sql_init_file()

    # Read the SQL schema file
    with open(sql_file, "r") as f:
        schema_sql = f.read()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Execute the entire schema
    cursor.executescript(schema_sql)

    conn.commit()
    conn.close()


def insert_folder(
    db_path: Path,
    title: str,
    user_id: str,
    created_at: datetime,
    updated_at: datetime,
    parent_id: Optional[str] = None,
) -> str:
    """Insert a folder and return its ID"""
    folder_id = str(uuid.uuid4())
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Use SQLite datetime format: YYYY-MM-DD HH:MM:SS (not ISO format with T)
    created_str = created_at.strftime("%Y-%m-%d %H:%M:%S")
    updated_str = updated_at.strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute(
        """
        INSERT INTO folders (id, title, parent_id, user_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (folder_id, title, parent_id, user_id, created_str, updated_str),
    )

    conn.commit()
    conn.close()
    return folder_id


def insert_note(
    db_path: Path,
    title: str,
    syntax: str,
    content: str,
    user_id: str,
    created_at: datetime,
    updated_at: datetime,
    parent_id: Optional[str] = None,
) -> str:
    """Insert a note and return its ID"""
    note_id = str(uuid.uuid4())
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Use SQLite datetime format: YYYY-MM-DD HH:MM:SS (not ISO format with T)
    created_str = created_at.strftime("%Y-%m-%d %H:%M:%S")
    updated_str = updated_at.strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute(
        """
        INSERT INTO notes (id, title, syntax, content, parent_id, user_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            note_id,
            title,
            syntax,
            content,
            parent_id,
            user_id,
            created_str,
            updated_str,
        ),
    )

    conn.commit()
    conn.close()
    return note_id


@app.command()
def basic(
    db_path: str = typer.Argument(..., help="Path to SQLite database file to create"),
    user_id: str = typer.Option("test_user", help="User ID to use in database"),
):
    """Create basic test database with varied timestamps"""
    db_file = Path(db_path)

    if db_file.exists():
        typer.echo(f"Database file already exists at {db_path}")
        if not typer.confirm("Overwrite?"):
            typer.echo("Cancelled")
            return
        db_file.unlink()

    print(f"\n{BLUE}Creating test database at {db_path}{RESET}\n")

    # Initialize schema
    try:
        init_schema(db_file)
        print(f"{GREEN}✓{RESET} Database schema loaded from sql/init.sql")
        print(f"  - Tables: folders, notes, notes_history")
        print(f"  - Views: v_folder_id_path_mapping, v_note_id_path_mapping")
        print(f"  - FTS: notes_fts (full-text search)")
        print(f"  - Triggers: history tracking for updates/deletes")
    except FileNotFoundError as e:
        print(f"\n{BLUE}Error:{RESET} {e}")
        return

    # Define base timestamps
    now = datetime.now()
    one_hour_ago = now - timedelta(hours=1)
    one_day_ago = now - timedelta(days=1)
    one_week_ago = now - timedelta(days=7)
    one_month_ago = now - timedelta(days=30)

    # Create test folders
    print(f"\n{BLUE}Creating test folders:{RESET}")

    root_folder_id = insert_folder(
        db_file,
        "documents",
        user_id,
        one_month_ago,
        one_week_ago,
    )
    print(f"{GREEN}✓{RESET} documents/ (created: 30d ago, modified: 7d ago)")

    projects_id = insert_folder(
        db_file,
        "projects",
        user_id,
        one_week_ago,
        one_day_ago,
    )
    print(f"{GREEN}✓{RESET} projects/ (created: 7d ago, modified: 1d ago)")

    archive_id = insert_folder(
        db_file,
        "archive",
        user_id,
        one_month_ago,
        one_month_ago,  # Never modified
    )
    print(f"{GREEN}✓{RESET} archive/ (created: 30d ago, modified: 30d ago)")

    work_id = insert_folder(
        db_file,
        "work",
        user_id,
        one_day_ago,
        one_hour_ago,
        parent_id=projects_id,
    )
    print(f"{GREEN}✓{RESET} projects/work/ (created: 1d ago, modified: 1h ago)")

    # Create test notes with varied timestamps
    print(f"\n{BLUE}Creating test notes:{RESET}")

    insert_note(
        db_file,
        "old_document",
        "md",
        "This is an old document.\n\nIt was created a month ago and never modified.",
        user_id,
        one_month_ago,
        one_month_ago,
        parent_id=root_folder_id,
    )
    print(f"{GREEN}✓{RESET} documents/old_document.md (created: 30d ago, modified: 30d ago)")

    insert_note(
        db_file,
        "recently_modified",
        "md",
        "This document was modified recently.\n\nIt was created a week ago but modified today.",
        user_id,
        one_week_ago,
        one_hour_ago,
        parent_id=root_folder_id,
    )
    print(f"{GREEN}✓{RESET} documents/recently_modified.md (created: 7d ago, modified: 1h ago)")

    insert_note(
        db_file,
        "new_document",
        "txt",
        "This is a new document created today.",
        user_id,
        one_hour_ago,
        one_hour_ago,
        parent_id=root_folder_id,
    )
    print(f"{GREEN}✓{RESET} documents/new_document.txt (created: 1h ago, modified: 1h ago)")

    insert_note(
        db_file,
        "readme",
        "md",
        "# Project README\n\nThis is the main project readme.",
        user_id,
        one_week_ago,
        one_day_ago,
        parent_id=projects_id,
    )
    print(f"{GREEN}✓{RESET} projects/readme.md (created: 7d ago, modified: 1d ago)")

    insert_note(
        db_file,
        "todo",
        "md",
        "# TODO List\n\n- [ ] Task 1\n- [ ] Task 2",
        user_id,
        one_day_ago,
        one_hour_ago,
        parent_id=work_id,
    )
    print(f"{GREEN}✓{RESET} projects/work/todo.md (created: 1d ago, modified: 1h ago)")

    insert_note(
        db_file,
        "archived",
        "py",
        "# Old Python Script\n\nprint('This script is archived')",
        user_id,
        one_month_ago,
        one_month_ago,
        parent_id=archive_id,
    )
    print(f"{GREEN}✓{RESET} archive/archived.py (created: 30d ago, modified: 30d ago)")

    print(f"\n{BLUE}{'='*60}{RESET}")
    print(f"{GREEN}✓ Test database created successfully{RESET}")
    print(f"{BLUE}{'='*60}{RESET}\n")

    print("Summary of test data:")
    print("- 4 folders with different creation/modification times")
    print("- 6 notes with varied timestamps")
    print("- Folder hierarchy: documents/, projects/, archive/, projects/work/")
    print("\nYou can now mount the FUSE filesystem and verify that:")
    print("- ctime matches created_at from database")
    print("- mtime matches updated_at from database")
    print("- Older files show older timestamps")
    print("- Files modified after creation show different ctime and mtime")


@app.command()
def edge_cases(
    db_path: str = typer.Argument(..., help="Path to SQLite database file to create"),
    user_id: str = typer.Option("test_user", help="User ID to use in database"),
):
    """Create database with edge case timestamps"""
    db_file = Path(db_path)

    if db_file.exists():
        typer.echo(f"Database file already exists at {db_path}")
        if not typer.confirm("Overwrite?"):
            typer.echo("Cancelled")
            return
        db_file.unlink()

    print(f"\n{BLUE}Creating edge case test database at {db_path}{RESET}\n")

    try:
        init_schema(db_file)
        print(f"{GREEN}✓{RESET} Database schema loaded from sql/init.sql")
    except FileNotFoundError as e:
        print(f"\n{BLUE}Error:{RESET} {e}")
        return

    # Edge case timestamps
    now = datetime.now()
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    epoch = datetime(1970, 1, 1)  # Unix epoch
    future = now + timedelta(days=365)  # Future date

    print(f"\n{BLUE}Creating edge case folders:{RESET}")

    # Same created and modified time
    insert_folder(db_file, "same_time", user_id, midnight, midnight)
    print(f"{GREEN}✓{RESET} same_time/ (created and modified at same time)")

    # Just created (created_at = updated_at = now)
    insert_folder(db_file, "just_created", user_id, now, now)
    print(f"{GREEN}✓{RESET} just_created/ (created and modified right now)")

    print(f"\n{BLUE}Creating edge case notes:{RESET}")

    # Large time gap between creation and modification
    insert_note(
        db_file,
        "old_created_recent_modified",
        "md",
        "Created long ago but modified very recently.",
        user_id,
        one_month_ago := now - timedelta(days=30),
        now,
    )
    print(f"{GREEN}✓{RESET} old_created_recent_modified.md (30 day time gap)")

    # Minimal difference
    insert_note(
        db_file,
        "minimal_diff",
        "txt",
        "Barely modified.",
        user_id,
        now - timedelta(seconds=1),
        now,
    )
    print(f"{GREEN}✓{RESET} minimal_diff.txt (1 second time gap)")

    # Same timestamp
    insert_note(
        db_file,
        "identical_times",
        "md",
        "This file has identical creation and modification time.",
        user_id,
        midnight,
        midnight,
    )
    print(f"{GREEN}✓{RESET} identical_times.md (identical timestamps)")

    # Very old file (if supported)
    try:
        insert_note(
            db_file,
            "very_old",
            "txt",
            "From the ancient times.",
            user_id,
            epoch,
            epoch,
        )
        print(f"{GREEN}✓{RESET} very_old.txt (epoch: 1970-01-01)")
    except Exception as e:
        print(f"⚠ very_old.txt skipped (epoch not supported): {e}")

    print(f"\n{BLUE}{'='*60}{RESET}")
    print(f"{GREEN}✓ Edge case database created successfully{RESET}")
    print(f"{BLUE}{'='*60}{RESET}\n")

    print("Edge cases to verify:")
    print("- Files where ctime == mtime (never modified)")
    print("- Files with large gaps between creation and modification")
    print("- Files with minimal time differences (< 1 second)")
    print("- Very old timestamps (epoch)")


@app.command()
def many_files(
    db_path: str = typer.Argument(..., help="Path to SQLite database file to create"),
    count: int = typer.Option(100, help="Number of files to create"),
    user_id: str = typer.Option("test_user", help="User ID to use in database"),
):
    """Create database with many files to test performance"""
    db_file = Path(db_path)

    if db_file.exists():
        typer.echo(f"Database file already exists at {db_path}")
        if not typer.confirm("Overwrite?"):
            typer.echo("Cancelled")
            return
        db_file.unlink()

    print(f"\n{BLUE}Creating database with {count} test files at {db_path}{RESET}\n")

    try:
        init_schema(db_file)
        print(f"{GREEN}✓{RESET} Database schema loaded from sql/init.sql")
    except FileNotFoundError as e:
        print(f"\n{BLUE}Error:{RESET} {e}")
        return

    # Create main folder
    main_folder_id = insert_folder(
        db_file,
        "test_files",
        user_id,
        datetime.now() - timedelta(days=1),
        datetime.now(),
    )
    print(f"{GREEN}✓{RESET} test_files/ folder created")

    now = datetime.now()
    print(f"\n{BLUE}Creating {count} test files:{RESET}")

    # Create many files with incremental timestamps
    for i in range(count):
        # Spread timestamps over the last 'count' hours
        file_time = now - timedelta(hours=count - i)

        insert_note(
            db_file,
            f"file_{i:04d}",
            "txt",
            f"Test file #{i}\n\nContent: {i * 'x'}",
            user_id,
            file_time,
            file_time,
            parent_id=main_folder_id,
        )

        if (i + 1) % 20 == 0:
            print(f"  Created {i + 1}/{count} files...")

    print(f"\n{BLUE}{'='*60}{RESET}")
    print(f"{GREEN}✓ Database with {count} files created successfully{RESET}")
    print(f"{BLUE}{'='*60}{RESET}\n")

    print(f"Files are timestamped from {now - timedelta(hours=count)} to {now}")
    print("Use this to test:")
    print("- Directory listing performance")
    print("- Timestamp ordering")
    print("- Reading many files")


@app.command()
def show(db_path: str = typer.Argument(..., help="Path to SQLite database file")):
    """Display contents of test database"""
    db_file = Path(db_path)

    if not db_file.exists():
        typer.echo(f"Database not found: {db_path}", err=True)
        return

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    print(f"\n{BLUE}{'='*60}{RESET}")
    print(f"Database: {db_path}")
    print(f"{BLUE}{'='*60}{RESET}\n")

    # Show folders
    print(f"{BLUE}Folders:{RESET}")
    cursor.execute(
        "SELECT id, title, parent_id, created_at, updated_at FROM folders ORDER BY title"
    )
    for row in cursor.fetchall():
        folder_id, title, parent_id, created_at, updated_at = row
        parent_str = f" (parent: {parent_id[:8]}...)" if parent_id else ""
        print(f"  {title}/{parent_str}")
        print(f"    Created:  {created_at}")
        print(f"    Modified: {updated_at}")

    # Show notes
    print(f"\n{BLUE}Notes:{RESET}")
    cursor.execute(
        "SELECT id, title, syntax, parent_id, created_at, updated_at FROM notes ORDER BY title"
    )
    for row in cursor.fetchall():
        note_id, title, syntax, parent_id, created_at, updated_at = row
        parent_str = f" (parent: {parent_id[:8]}...)" if parent_id else ""
        print(f"  {title}.{syntax}{parent_str}")
        print(f"    Created:  {created_at}")
        print(f"    Modified: {updated_at}")

    conn.close()
    print(f"\n{BLUE}{'='*60}{RESET}\n")


if __name__ == "__main__":
    app()
