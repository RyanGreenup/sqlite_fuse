use chrono::{DateTime, NaiveDateTime, Utc};
use chrono_tz::Tz;
use rusqlite::{Connection, Result, params};

pub struct Database {
    pub connection: Connection,
    pub timezone: Tz,
}

impl Database {
    pub fn new(connection: Connection, timezone: Option<Tz>) -> Self {
        Self {
            connection,
            timezone: timezone.unwrap_or(chrono_tz::UTC),
        }
    }

    pub fn with_utc(connection: Connection) -> Self {
        Self::new(connection, None)
    }
    pub fn create_folder(&self, title: &str, parent_id: Option<&str>) -> Result<String> {
        let id = format!("{:x}", uuid::Uuid::new_v4().as_simple());
        let now = Utc::now()
            .with_timezone(&self.timezone)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();

        self.connection.execute(
            "INSERT INTO folders (id, title, parent_id, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?4)",
            params![id, title, parent_id, now],
        )?;

        Ok(id)
    }

    pub fn get_folder_by_id(&self, id: &str) -> Result<Option<Folder>> {
        let mut stmt = self.connection.prepare(
            "SELECT id, title, parent_id, created_at, updated_at FROM folders WHERE id = ?1",
        )?;

        let mut folder_iter = stmt.query_map([id], |row| {
            Ok(Folder {
                id: row.get(0)?,
                title: row.get(1)?,
                parent_id: row.get(2)?,
                created_at: NaiveDateTime::parse_from_str(
                    &row.get::<_, String>(3)?,
                    "%Y-%m-%d %H:%M:%S",
                )
                .map_err(|_| {
                    rusqlite::Error::InvalidColumnType(
                        3,
                        "created_at".to_string(),
                        rusqlite::types::Type::Text,
                    )
                })?
                .and_utc(),
                updated_at: NaiveDateTime::parse_from_str(
                    &row.get::<_, String>(4)?,
                    "%Y-%m-%d %H:%M:%S",
                )
                .map_err(|_| {
                    rusqlite::Error::InvalidColumnType(
                        4,
                        "updated_at".to_string(),
                        rusqlite::types::Type::Text,
                    )
                })?
                .and_utc(),
            })
        })?;

        match folder_iter.next() {
            Some(folder) => Ok(Some(folder?)),
            None => Ok(None),
        }
    }

    pub fn update_folder(&self, id: &str, title: &str) -> Result<bool> {
        let now = Utc::now()
            .with_timezone(&self.timezone)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        let rows_affected = self.connection.execute(
            "UPDATE folders SET title = ?1, updated_at = ?2 WHERE id = ?3",
            params![title, now, id],
        )?;

        Ok(rows_affected > 0)
    }

    pub fn delete_folder(&self, id: &str) -> Result<bool> {
        let rows_affected = self
            .connection
            .execute("DELETE FROM folders WHERE id = ?1", params![id])?;

        Ok(rows_affected > 0)
    }

    pub fn list_folders_by_parent(&self, parent_id: Option<&str>) -> Result<Vec<Folder>> {
        let query = match parent_id {
            Some(_) => {
                "SELECT id, title, parent_id, created_at, updated_at FROM folders WHERE parent_id = ?1 ORDER BY title"
            }
            None => {
                "SELECT id, title, parent_id, created_at, updated_at FROM folders WHERE parent_id IS NULL ORDER BY title"
            }
        };

        let mut stmt = self.connection.prepare(query)?;
        let folder_iter = match parent_id {
            Some(pid) => stmt.query_map([pid], Self::map_folder_row)?,
            None => stmt.query_map([], Self::map_folder_row)?,
        };

        folder_iter.collect()
    }

    pub fn create_note(
        &self,
        id: &str,
        title: &str,
        abstract_text: Option<&str>,
        content: &str,
        syntax: &str,
        parent_id: Option<&str>,
        user_id: &str,
    ) -> Result<String> {
        let now = Utc::now()
            .with_timezone(&self.timezone)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();

        self.connection.execute(
            "INSERT INTO notes (id, title, abstract, content, syntax, parent_id, user_id, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
            params![id, title, abstract_text, content, syntax, parent_id, user_id, now],
        )?;

        Ok(id.to_string())
    }

    pub fn get_note_by_id(&self, id: &str) -> Result<Option<Note>> {
        let mut stmt = self.connection.prepare(
            "SELECT id, title, abstract, content, syntax, parent_id, user_id, created_at, updated_at
             FROM notes WHERE id = ?1",
        )?;

        let mut note_iter = stmt.query_map([id], Self::map_note_row)?;

        match note_iter.next() {
            Some(note) => Ok(Some(note?)),
            None => Ok(None),
        }
    }

    pub fn update_note(
        &self,
        id: &str,
        title: &str,
        abstract_text: Option<&str>,
        content: &str,
        syntax: &str,
    ) -> Result<bool> {
        let now = Utc::now()
            .with_timezone(&self.timezone)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();

        let rows_affected = self.connection.execute(
            "UPDATE notes SET title = ?1, abstract = ?2, content = ?3, syntax = ?4, updated_at = ?5
             WHERE id = ?6",
            params![title, abstract_text, content, syntax, now, id],
        )?;

        Ok(rows_affected > 0)
    }

    pub fn update_note_parent(&self, id: &str, parent_id: Option<&str>) -> Result<bool> {
        let now = Utc::now()
            .with_timezone(&self.timezone)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();

        let rows_affected = self.connection.execute(
            "UPDATE notes SET parent_id = ?1, updated_at = ?2 WHERE id = ?3",
            params![parent_id, now, id],
        )?;

        Ok(rows_affected > 0)
    }

    pub fn delete_note(&self, id: &str) -> Result<bool> {
        let rows_affected = self
            .connection
            .execute("DELETE FROM notes WHERE id = ?1", params![id])?;

        Ok(rows_affected > 0)
    }

    pub fn list_notes_by_parent(
        &self,
        parent_id: Option<&str>,
        user_id: &str,
    ) -> Result<Vec<Note>> {
        let query = match parent_id {
            Some(_) => "SELECT id, title, abstract, content, syntax, parent_id, user_id, created_at, updated_at
                       FROM notes WHERE parent_id = ?1 AND user_id = ?2 ORDER BY title",
            None => "SELECT id, title, abstract, content, syntax, parent_id, user_id, created_at, updated_at
                    FROM notes WHERE parent_id IS NULL AND user_id = ?1 ORDER BY title"
        };

        let mut stmt = self.connection.prepare(query)?;
        let note_iter = match parent_id {
            Some(pid) => stmt.query_map(params![pid, user_id], Self::map_note_row)?,
            None => stmt.query_map([user_id], Self::map_note_row)?,
        };

        note_iter.collect()
    }

    pub fn get_folder_path_by_id(&self, id: &str) -> Result<Option<String>> {
        let mut stmt = self
            .connection
            .prepare("SELECT full_path FROM v_folder_id_path_mapping WHERE id = ?1")?;

        let mut path_iter = stmt.query_map([id], |row| Ok(row.get::<_, String>(0)?))?;

        match path_iter.next() {
            Some(path) => Ok(Some(path?)),
            None => Ok(None),
        }
    }

    pub fn get_folder_id_by_path(&self, path: &str) -> Result<Option<String>> {
        println!("[get_folder_id_by_path] Looking for {path}");
        println!("[get_folder_id_by_path] Debugging v_folder_id_path_mapping:");
        let mut debug_stmt = self
            .connection
            .prepare("SELECT full_path, id FROM v_folder_id_path_mapping")?;
        let debug_iter = debug_stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        for result in debug_iter {
            let (path, id) = result?;
            println!("[get_folder_id_by_path] Path: '{}', ID: '{}'", path, id);
        }
        let mut stmt = self
            .connection
            .prepare("SELECT id FROM v_folder_id_path_mapping WHERE full_path = ?1")?;

        let mut id_iter = stmt.query_map([path], |row| Ok(row.get::<_, String>(0)?))?;

        match id_iter.next() {
            Some(id) => Ok(Some(id?)),
            None => Ok(None),
        }
    }

    pub fn get_note_path_by_id(&self, id: &str) -> Result<Option<String>> {
        let mut stmt = self
            .connection
            .prepare("SELECT full_path FROM v_note_id_path_mapping WHERE id = ?1")?;

        let mut path_iter = stmt.query_map([id], |row| Ok(row.get::<_, String>(0)?))?;

        match path_iter.next() {
            Some(path) => Ok(Some(path?)),
            None => Ok(None),
        }
    }

    pub fn get_note_id_by_path(&self, path: &str) -> Result<Option<String>> {
        let mut stmt = self
            .connection
            .prepare("SELECT id FROM v_note_id_path_mapping WHERE full_path = ?1")?;

        let mut id_iter = stmt.query_map([path], |row| Ok(row.get::<_, String>(0)?))?;

        match id_iter.next() {
            Some(id) => Ok(Some(id?)),
            None => Ok(None),
        }
    }

    pub fn get_folder_contents_recursive(
        &self,
        folder_id: &str,
        user_id: &str,
    ) -> Result<Vec<FileType>> {
        let query = "
            WITH RECURSIVE folder_tree AS (
                -- Base case: the specified folder
                SELECT
                    id,
                    title,
                    parent_id,
                    0 as depth,
                    title as path
                FROM folders
                WHERE id = ?1

                UNION ALL

                -- Recursive case: child folders
                SELECT
                    f.id,
                    f.title,
                    f.parent_id,
                    ft.depth + 1 as depth,
                    CASE
                        WHEN ft.depth = 0 THEN f.title
                        ELSE ft.path || '/' || f.title
                    END as path
                FROM folders f
                INNER JOIN folder_tree ft ON f.parent_id = ft.id
            ),
            folder_paths AS (
                SELECT
                    id,
                    path,
                    'directory' as type
                FROM folder_tree
                WHERE depth > 0  -- Exclude the root folder itself
            ),
            note_paths AS (
                SELECT
                    n.id,
                    CASE
                        WHEN ft.depth = 0 THEN n.title || '.' || n.syntax
                        ELSE ft.path || '/' || n.title || '.' || n.syntax
                    END as path,
                    'file' as type
                FROM notes n
                INNER JOIN folder_tree ft ON (n.parent_id = ft.id OR (n.parent_id IS NULL AND ft.id = ?1))
                WHERE n.user_id = ?2
            )
            SELECT path, type FROM folder_paths
            UNION ALL
            SELECT path, type FROM note_paths
            ORDER BY path";

        let mut stmt = self.connection.prepare(query)?;
        let file_iter = stmt.query_map(params![folder_id, user_id], |row| {
            let path: String = row.get(0)?;
            let file_type: String = row.get(1)?;

            match file_type.as_str() {
                "directory" => Ok(FileType::Directory { path }),
                "file" => Ok(FileType::File { path }),
                _ => Err(rusqlite::Error::InvalidColumnType(
                    1,
                    "type".to_string(),
                    rusqlite::types::Type::Text,
                )),
            }
        })?;

        file_iter.collect()
    }

    pub fn get_child_count(
        &self,
        parent_id: Option<&str>,
        user_id: Option<&str>,
    ) -> Result<(usize, usize)> {
        let (folder_count, note_count) = match parent_id {
            Some(pid) => {
                // Count folders with this parent
                let folder_count: i64 = self.connection.query_row(
                    "SELECT COUNT(*) FROM folders WHERE parent_id = ?1",
                    [pid],
                    |row| row.get(0),
                )?;

                // Count notes with this parent (optionally filtered by user)
                let note_count: i64 = match user_id {
                    Some(uid) => self.connection.query_row(
                        "SELECT COUNT(*) FROM notes WHERE parent_id = ?1 AND user_id = ?2",
                        params![pid, uid],
                        |row| row.get(0),
                    )?,
                    None => self.connection.query_row(
                        "SELECT COUNT(*) FROM notes WHERE parent_id = ?1",
                        [pid],
                        |row| row.get(0),
                    )?,
                };

                (folder_count, note_count)
            }
            None => {
                // Count root folders (no parent)
                let folder_count: i64 = self.connection.query_row(
                    "SELECT COUNT(*) FROM folders WHERE parent_id IS NULL",
                    [],
                    |row| row.get(0),
                )?;

                // Count root notes (optionally filtered by user)
                let note_count: i64 = match user_id {
                    Some(uid) => self.connection.query_row(
                        "SELECT COUNT(*) FROM notes WHERE parent_id IS NULL AND user_id = ?1",
                        [uid],
                        |row| row.get(0),
                    )?,
                    None => self.connection.query_row(
                        "SELECT COUNT(*) FROM notes WHERE parent_id IS NULL",
                        [],
                        |row| row.get(0),
                    )?,
                };

                (folder_count, note_count)
            }
        };

        Ok((folder_count as usize, note_count as usize))
    }

    /// Maps a database row to a Folder struct, handling datetime parsing.
    /// Extracted as a helper to avoid code duplication across query methods.
    fn map_folder_row(row: &rusqlite::Row) -> rusqlite::Result<Folder> {
        Ok(Folder {
            id: row.get(0)?,
            title: row.get(1)?,
            parent_id: row.get(2)?,
            created_at: NaiveDateTime::parse_from_str(
                &row.get::<_, String>(3)?,
                "%Y-%m-%d %H:%M:%S",
            )
            .map_err(|_| {
                rusqlite::Error::InvalidColumnType(
                    3,
                    "created_at".to_string(),
                    rusqlite::types::Type::Text,
                )
            })?
            .and_utc(),
            updated_at: NaiveDateTime::parse_from_str(
                &row.get::<_, String>(4)?,
                "%Y-%m-%d %H:%M:%S",
            )
            .map_err(|_| {
                rusqlite::Error::InvalidColumnType(
                    4,
                    "updated_at".to_string(),
                    rusqlite::types::Type::Text,
                )
            })?
            .and_utc(),
        })
    }

    /// Maps a database row to a Note struct, handling datetime parsing.
    /// Extracted as a helper to avoid code duplication across query methods.
    fn map_note_row(row: &rusqlite::Row) -> rusqlite::Result<Note> {
        Ok(Note {
            id: row.get(0)?,
            title: row.get(1)?,
            abstract_text: row.get(2)?,
            content: row.get(3)?,
            syntax: row.get(4)?,
            parent_id: row.get(5)?,
            user_id: row.get(6)?,
            created_at: NaiveDateTime::parse_from_str(
                &row.get::<_, String>(7)?,
                "%Y-%m-%d %H:%M:%S",
            )
            .map_err(|_| {
                rusqlite::Error::InvalidColumnType(
                    7,
                    "created_at".to_string(),
                    rusqlite::types::Type::Text,
                )
            })?
            .and_utc(),
            updated_at: NaiveDateTime::parse_from_str(
                &row.get::<_, String>(8)?,
                "%Y-%m-%d %H:%M:%S",
            )
            .map_err(|_| {
                rusqlite::Error::InvalidColumnType(
                    8,
                    "updated_at".to_string(),
                    rusqlite::types::Type::Text,
                )
            })?
            .and_utc(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Folder {
    pub id: String,
    pub title: String,
    pub parent_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct Note {
    pub id: String,
    pub title: String,
    pub abstract_text: Option<String>,
    pub content: String,
    pub syntax: String,
    pub parent_id: Option<String>,
    pub user_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub enum FileType {
    Directory { path: String },
    File { path: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono_tz;
    use rusqlite::Connection;

    fn setup_test_database() -> Database {
        let conn = Connection::open_in_memory().expect("Failed to create in-memory database");

        // Read and execute the init.sql file
        let init_sql = include_str!("../sql/init.sql");
        conn.execute_batch(init_sql)
            .expect("Failed to initialize database");

        Database::new(conn, Some(chrono_tz::Australia::Sydney))
    }

    #[test]
    fn test_folder_crud_operations() {
        let db = setup_test_database();

        // Create root folder
        let root_id = db
            .create_folder("Documents", None)
            .expect("Failed to create root folder");
        assert!(!root_id.is_empty());

        // Create child folder
        let child_id = db
            .create_folder("Projects", Some(&root_id))
            .expect("Failed to create child folder");
        assert!(!child_id.is_empty());
        assert_ne!(root_id, child_id);

        // Read back the root folder
        let folder = db
            .get_folder_by_id(&root_id)
            .expect("Failed to query folder")
            .expect("Root folder not found");

        assert_eq!(folder.id, root_id);
        assert_eq!(folder.title, "Documents");
        assert_eq!(folder.parent_id, None);
        assert!(!folder.created_at.to_string().is_empty());
        assert!(!folder.updated_at.to_string().is_empty());

        // Read back the child folder
        let child_folder = db
            .get_folder_by_id(&child_id)
            .expect("Failed to query child folder")
            .expect("Child folder not found");

        assert_eq!(child_folder.id, child_id);
        assert_eq!(child_folder.title, "Projects");
        assert_eq!(child_folder.parent_id, Some(root_id.clone()));

        // Test update
        let updated = db
            .update_folder(&root_id, "My Documents")
            .expect("Failed to update folder");
        assert!(updated);

        let updated_folder = db
            .get_folder_by_id(&root_id)
            .expect("Failed to query updated folder")
            .expect("Updated folder not found");
        assert_eq!(updated_folder.title, "My Documents");

        // Test list folders by parent
        let children = db
            .list_folders_by_parent(Some(&root_id))
            .expect("Failed to list child folders");
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].title, "Projects");

        // Test delete
        let deleted = db
            .delete_folder(&child_id)
            .expect("Failed to delete folder");
        assert!(deleted);

        let deleted_folder = db
            .get_folder_by_id(&child_id)
            .expect("Failed to query deleted folder");
        assert!(deleted_folder.is_none());
    }

    #[test]
    fn test_timezone_functionality() {
        let db = setup_test_database();

        let folder_id = db
            .create_folder("Test Timezone", None)
            .expect("Failed to create folder for timezone test");

        let folder = db
            .get_folder_by_id(&folder_id)
            .expect("Failed to query folder")
            .expect("Folder not found");

        // The timestamps should be stored and retrieved properly
        // Note: The actual timezone conversion happens during storage
        assert!(!folder.created_at.to_string().is_empty());
        assert!(!folder.updated_at.to_string().is_empty());
        assert_eq!(folder.created_at, folder.updated_at);
    }

    #[test]
    fn test_get_note_by_id() {
        let db = setup_test_database();
        let user_id = "test_user_123";
        let note_id = "test_note_456";

        // Test getting non-existent note
        let non_existent = db
            .get_note_by_id("non_existent_id")
            .expect("Failed to query non-existent note");
        assert!(non_existent.is_none());

        // Create a note directly in database for testing
        db.connection.execute(
            "INSERT INTO notes (id, title, abstract, content, syntax, parent_id, user_id, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
            params![note_id, "Test Note", "Test abstract", "# Test Content", "md", None::<String>, user_id, "2023-01-01 12:00:00"]
        ).expect("Failed to insert test note");

        // Test getting existing note
        let note = db
            .get_note_by_id(note_id)
            .expect("Failed to query note")
            .expect("Note not found");

        assert_eq!(note.id, note_id);
        assert_eq!(note.title, "Test Note");
        assert_eq!(note.abstract_text, Some("Test abstract".to_string()));
        assert_eq!(note.content, "# Test Content");
        assert_eq!(note.syntax, "md");
        assert_eq!(note.parent_id, None);
        assert_eq!(note.user_id, user_id);
    }

    #[test]
    fn test_create_note() {
        let db = setup_test_database();
        let user_id = "test_user_456";

        // Create note without parent
        let note_id = db
            .create_note(
                "note_1",
                "My First Note",
                Some("An abstract"),
                "# Hello World",
                "md",
                None,
                user_id,
            )
            .expect("Failed to create note");
        assert_eq!(note_id, "note_1");

        // Verify note was created
        let note = db
            .get_note_by_id("note_1")
            .expect("Failed to query created note")
            .expect("Created note not found");

        assert_eq!(note.title, "My First Note");
        assert_eq!(note.abstract_text, Some("An abstract".to_string()));
        assert_eq!(note.content, "# Hello World");
        assert_eq!(note.syntax, "md");
        assert_eq!(note.user_id, user_id);

        // Create note with parent folder
        let folder_id = db
            .create_folder("Notes Folder", None)
            .expect("Failed to create folder");

        let note_id2 = db
            .create_note(
                "note_2",
                "Second Note",
                None,
                "Some content",
                "txt",
                Some(&folder_id),
                user_id,
            )
            .expect("Failed to create note with parent");

        let note2 = db
            .get_note_by_id(&note_id2)
            .expect("Failed to query second note")
            .expect("Second note not found");

        assert_eq!(note2.parent_id, Some(folder_id));
        assert_eq!(note2.abstract_text, None);
        assert_eq!(note2.syntax, "txt");
    }

    #[test]
    fn test_update_note_content() {
        let db = setup_test_database();
        let user_id = "test_user_789";

        let note_id = db
            .create_note(
                "update_test",
                "Original Title",
                Some("Original abstract"),
                "Original content",
                "md",
                None,
                user_id,
            )
            .expect("Failed to create note for update test");

        // Test updating all content fields
        let updated = db
            .update_note(
                &note_id,
                "Updated Title",
                Some("Updated abstract"),
                "Updated content",
                "rst",
            )
            .expect("Failed to update note");
        assert!(updated);

        let note = db
            .get_note_by_id(&note_id)
            .expect("Failed to query updated note")
            .expect("Updated note not found");

        assert_eq!(note.title, "Updated Title");
        assert_eq!(note.abstract_text, Some("Updated abstract".to_string()));
        assert_eq!(note.content, "Updated content");
        assert_eq!(note.syntax, "rst");

        // Test updating with None abstract
        let updated2 = db
            .update_note(&note_id, "Final Title", None, "Final content", "md")
            .expect("Failed to update note with None abstract");
        assert!(updated2);

        let note2 = db
            .get_note_by_id(&note_id)
            .expect("Failed to query updated note")
            .expect("Updated note not found");

        assert_eq!(note2.abstract_text, None);

        // Test updating non-existent note
        let not_updated = db
            .update_note("non_existent", "Title", None, "Content", "md")
            .expect("Failed to update non-existent note");
        assert!(!not_updated);
    }

    #[test]
    fn test_update_note_parent() {
        let db = setup_test_database();
        let user_id = "test_user_parent";

        let note_id = db
            .create_note(
                "parent_test",
                "Test Note",
                None,
                "Content",
                "md",
                None,
                user_id,
            )
            .expect("Failed to create note for parent test");

        // Create folders
        let folder1_id = db
            .create_folder("Folder 1", None)
            .expect("Failed to create folder 1");
        let folder2_id = db
            .create_folder("Folder 2", None)
            .expect("Failed to create folder 2");

        // Move note to folder 1
        let updated = db
            .update_note_parent(&note_id, Some(&folder1_id))
            .expect("Failed to update note parent");
        assert!(updated);

        let note = db
            .get_note_by_id(&note_id)
            .expect("Failed to query note")
            .expect("Note not found");
        assert_eq!(note.parent_id, Some(folder1_id.clone()));

        // Move note to folder 2
        let updated2 = db
            .update_note_parent(&note_id, Some(&folder2_id))
            .expect("Failed to update note parent to folder 2");
        assert!(updated2);

        let note2 = db
            .get_note_by_id(&note_id)
            .expect("Failed to query note")
            .expect("Note not found");
        assert_eq!(note2.parent_id, Some(folder2_id));

        // Move note to root (no parent)
        let updated3 = db
            .update_note_parent(&note_id, None)
            .expect("Failed to update note parent to None");
        assert!(updated3);

        let note3 = db
            .get_note_by_id(&note_id)
            .expect("Failed to query note")
            .expect("Note not found");
        assert_eq!(note3.parent_id, None);

        // Test updating non-existent note
        let not_updated = db
            .update_note_parent("non_existent", Some(&folder1_id))
            .expect("Failed to update non-existent note parent");
        assert!(!not_updated);
    }

    #[test]
    fn test_delete_note() {
        let db = setup_test_database();
        let user_id = "test_user_delete";

        let note_id = db
            .create_note(
                "delete_test",
                "Delete Me",
                Some("Abstract"),
                "Content to delete",
                "md",
                None,
                user_id,
            )
            .expect("Failed to create note for delete test");

        // Verify note exists
        let note = db
            .get_note_by_id(&note_id)
            .expect("Failed to query note before delete")
            .expect("Note should exist before delete");
        assert_eq!(note.title, "Delete Me");

        // Delete note
        let deleted = db.delete_note(&note_id).expect("Failed to delete note");
        assert!(deleted);

        // Verify note is gone
        let deleted_note = db
            .get_note_by_id(&note_id)
            .expect("Failed to query deleted note");
        assert!(deleted_note.is_none());

        // Verify history was created (check if history table has the note)
        let mut stmt = db
            .connection
            .prepare("SELECT COUNT(*) FROM notes_history WHERE id = ?1 AND log_action = 'DELETE'")
            .expect("Failed to prepare history query");
        let count: i64 = stmt
            .query_row([&note_id], |row| row.get(0))
            .expect("Failed to query history count");
        assert_eq!(count, 1, "Note should be in history after deletion");

        // Test deleting non-existent note
        let not_deleted = db
            .delete_note("non_existent")
            .expect("Failed to delete non-existent note");
        assert!(!not_deleted);
    }

    #[test]
    fn test_list_notes_by_parent() {
        let db = setup_test_database();
        let user_id = "test_user_list";

        // Create folder
        let folder_id = db
            .create_folder("Test Folder", None)
            .expect("Failed to create folder");

        // Create notes in folder
        db.create_note(
            "note1",
            "Note 1",
            None,
            "Content 1",
            "md",
            Some(&folder_id),
            user_id,
        )
        .expect("Failed to create note 1");
        db.create_note(
            "note2",
            "Note 2",
            None,
            "Content 2",
            "txt",
            Some(&folder_id),
            user_id,
        )
        .expect("Failed to create note 2");

        // Create note at root level
        db.create_note(
            "note3",
            "Root Note",
            None,
            "Root content",
            "md",
            None,
            user_id,
        )
        .expect("Failed to create root note");

        // Create note for different user in same folder
        db.create_note(
            "note4",
            "Other User",
            None,
            "Content",
            "md",
            Some(&folder_id),
            "other_user",
        )
        .expect("Failed to create note for other user");

        // Test listing notes in folder for specific user
        let folder_notes = db
            .list_notes_by_parent(Some(&folder_id), user_id)
            .expect("Failed to list notes in folder");
        assert_eq!(folder_notes.len(), 2);

        let titles: Vec<&str> = folder_notes.iter().map(|n| n.title.as_str()).collect();
        assert!(titles.contains(&"Note 1"));
        assert!(titles.contains(&"Note 2"));
        assert!(!titles.contains(&"Other User"));

        // Test listing root notes for user
        let root_notes = db
            .list_notes_by_parent(None, user_id)
            .expect("Failed to list root notes");
        assert_eq!(root_notes.len(), 1);
        assert_eq!(root_notes[0].title, "Root Note");

        // Test listing notes for user with no notes in folder
        let empty_folder_id = db
            .create_folder("Empty Folder", None)
            .expect("Failed to create empty folder");
        let empty_notes = db
            .list_notes_by_parent(Some(&empty_folder_id), user_id)
            .expect("Failed to list notes in empty folder");
        assert_eq!(empty_notes.len(), 0);
    }

    #[test]
    fn test_fts_triggers() {
        let db = setup_test_database();
        let user_id = "fts_test_user";

        // Create a note
        let note_id = db
            .create_note(
                "fts_test",
                "Searchable Note",
                Some("This is searchable"),
                "Content with keywords",
                "md",
                None,
                user_id,
            )
            .expect("Failed to create note for FTS test");

        // Verify note was added to FTS table
        let mut stmt = db
            .connection
            .prepare("SELECT COUNT(*) FROM notes_fts WHERE id = ?1")
            .expect("Failed to prepare FTS query");
        let count: i64 = stmt
            .query_row([&note_id], |row| row.get(0))
            .expect("Failed to query FTS count");
        assert_eq!(count, 1, "Note should be in FTS table after creation");

        // Update the note and verify FTS is updated
        db.update_note(
            &note_id,
            "Updated Searchable",
            Some("Updated abstract"),
            "Updated content",
            "md",
        )
        .expect("Failed to update note");

        let mut stmt = db
            .connection
            .prepare("SELECT title FROM notes_fts WHERE id = ?1")
            .expect("Failed to prepare FTS query");
        let title: String = stmt
            .query_row([&note_id], |row| row.get(0))
            .expect("Failed to query FTS title");
        assert_eq!(
            title, "Updated Searchable",
            "FTS should reflect updated title"
        );

        // Delete the note and verify it's removed from FTS
        db.delete_note(&note_id).expect("Failed to delete note");

        let mut stmt = db
            .connection
            .prepare("SELECT COUNT(*) FROM notes_fts WHERE id = ?1")
            .expect("Failed to prepare FTS query");
        let count: i64 = stmt
            .query_row([&note_id], |row| row.get(0))
            .expect("Failed to query FTS count");
        assert_eq!(
            count, 0,
            "Note should be removed from FTS table after deletion"
        );
    }

    #[test]
    fn test_folder_path_resolution() {
        let db = setup_test_database();

        // Create nested folder structure: Documents/Projects/MyProject
        let docs_id = db
            .create_folder("Documents", None)
            .expect("Failed to create Documents folder");
        let projects_id = db
            .create_folder("Projects", Some(&docs_id))
            .expect("Failed to create Projects folder");
        let myproject_id = db
            .create_folder("MyProject", Some(&projects_id))
            .expect("Failed to create MyProject folder");

        // Test getting paths by ID
        let docs_path = db
            .get_folder_path_by_id(&docs_id)
            .expect("Failed to get Documents path")
            .expect("Documents path should exist");
        assert_eq!(docs_path, "Documents");

        let projects_path = db
            .get_folder_path_by_id(&projects_id)
            .expect("Failed to get Projects path")
            .expect("Projects path should exist");
        assert_eq!(projects_path, "Documents/Projects");

        let myproject_path = db
            .get_folder_path_by_id(&myproject_id)
            .expect("Failed to get MyProject path")
            .expect("MyProject path should exist");
        assert_eq!(myproject_path, "Documents/Projects/MyProject");

        // Test getting IDs by path
        let docs_id_resolved = db
            .get_folder_id_by_path("Documents")
            .expect("Failed to resolve Documents ID")
            .expect("Documents ID should exist");
        assert_eq!(docs_id_resolved, docs_id);

        let projects_id_resolved = db
            .get_folder_id_by_path("Documents/Projects")
            .expect("Failed to resolve Projects ID")
            .expect("Projects ID should exist");
        assert_eq!(projects_id_resolved, projects_id);

        let myproject_id_resolved = db
            .get_folder_id_by_path("Documents/Projects/MyProject")
            .expect("Failed to resolve MyProject ID")
            .expect("MyProject ID should exist");
        assert_eq!(myproject_id_resolved, myproject_id);

        // Test non-existent paths/IDs
        let non_existent_path = db
            .get_folder_path_by_id("non_existent_id")
            .expect("Failed to query non-existent folder path");
        assert!(non_existent_path.is_none());

        let non_existent_id = db
            .get_folder_id_by_path("Non/Existent/Path")
            .expect("Failed to query non-existent folder ID");
        assert!(non_existent_id.is_none());
    }

    #[test]
    fn test_note_path_resolution() {
        let db = setup_test_database();
        let user_id = "path_test_user";

        // Create folder structure and notes
        let work_id = db
            .create_folder("Work", None)
            .expect("Failed to create Work folder");
        let projects_id = db
            .create_folder("Projects", Some(&work_id))
            .expect("Failed to create Projects folder");

        // Create notes at different levels
        let root_note_id = db
            .create_note(
                "root_note",
                "README",
                None,
                "Root readme content",
                "md",
                None,
                user_id,
            )
            .expect("Failed to create root note");

        let work_note_id = db
            .create_note(
                "work_note",
                "agenda",
                None,
                "Work agenda",
                "org",
                Some(&work_id),
                user_id,
            )
            .expect("Failed to create work note");

        let project_note_id = db
            .create_note(
                "project_note",
                "specification",
                None,
                "Project spec",
                "txt",
                Some(&projects_id),
                user_id,
            )
            .expect("Failed to create project note");

        // Test getting paths by ID
        let root_path = db
            .get_note_path_by_id(&root_note_id)
            .expect("Failed to get root note path")
            .expect("Root note path should exist");
        assert_eq!(root_path, "README.md");

        let work_path = db
            .get_note_path_by_id(&work_note_id)
            .expect("Failed to get work note path")
            .expect("Work note path should exist");
        assert_eq!(work_path, "Work/agenda.org");

        let project_path = db
            .get_note_path_by_id(&project_note_id)
            .expect("Failed to get project note path")
            .expect("Project note path should exist");
        assert_eq!(project_path, "Work/Projects/specification.txt");

        // Test getting IDs by path
        let root_id_resolved = db
            .get_note_id_by_path("README.md")
            .expect("Failed to resolve root note ID")
            .expect("Root note ID should exist");
        assert_eq!(root_id_resolved, root_note_id);

        let work_id_resolved = db
            .get_note_id_by_path("Work/agenda.org")
            .expect("Failed to resolve work note ID")
            .expect("Work note ID should exist");
        assert_eq!(work_id_resolved, work_note_id);

        let project_id_resolved = db
            .get_note_id_by_path("Work/Projects/specification.txt")
            .expect("Failed to resolve project note ID")
            .expect("Project note ID should exist");
        assert_eq!(project_id_resolved, project_note_id);

        // Test non-existent paths/IDs
        let non_existent_path = db
            .get_note_path_by_id("non_existent_note")
            .expect("Failed to query non-existent note path");
        assert!(non_existent_path.is_none());

        let non_existent_id = db
            .get_note_id_by_path("Non/Existent/note.md")
            .expect("Failed to query non-existent note ID");
        assert!(non_existent_id.is_none());
    }

    #[test]
    fn test_path_resolution_edge_cases() {
        let db = setup_test_database();
        let user_id = "edge_case_user";

        // Test folder with special characters in name
        let special_id = db
            .create_folder("Folder-With_Special.Chars", None)
            .expect("Failed to create folder with special chars");

        let special_path = db
            .get_folder_path_by_id(&special_id)
            .expect("Failed to get special folder path")
            .expect("Special folder path should exist");
        assert_eq!(special_path, "Folder-With_Special.Chars");

        let special_id_resolved = db
            .get_folder_id_by_path("Folder-With_Special.Chars")
            .expect("Failed to resolve special folder ID")
            .expect("Special folder ID should exist");
        assert_eq!(special_id_resolved, special_id);

        // Test note with different syntax extensions
        let py_note_id = db
            .create_note(
                "python_script",
                "script",
                None,
                "print('hello')",
                "py",
                None,
                user_id,
            )
            .expect("Failed to create Python note");

        let py_path = db
            .get_note_path_by_id(&py_note_id)
            .expect("Failed to get Python note path")
            .expect("Python note path should exist");
        assert_eq!(py_path, "script.py");

        let py_id_resolved = db
            .get_note_id_by_path("script.py")
            .expect("Failed to resolve Python note ID")
            .expect("Python note ID should exist");
        assert_eq!(py_id_resolved, py_note_id);

        // Test deeply nested structure (5 levels)
        let mut current_parent = None;
        let mut expected_path = String::new();

        for i in 1..=5 {
            let folder_name = format!("Level{}", i);
            let folder_id = db
                .create_folder(&folder_name, current_parent.as_deref())
                .expect("Failed to create nested folder");

            if expected_path.is_empty() {
                expected_path = folder_name.clone();
            } else {
                expected_path = format!("{}/{}", expected_path, folder_name);
            }

            let resolved_path = db
                .get_folder_path_by_id(&folder_id)
                .expect("Failed to get nested folder path")
                .expect("Nested folder path should exist");
            assert_eq!(resolved_path, expected_path);

            current_parent = Some(folder_id);
        }

        // Create note in deeply nested folder
        let deep_note_id = db
            .create_note(
                "deep_note",
                "deep",
                None,
                "Deep content",
                "md",
                current_parent.as_deref(),
                user_id,
            )
            .expect("Failed to create deep note");

        let deep_path = db
            .get_note_path_by_id(&deep_note_id)
            .expect("Failed to get deep note path")
            .expect("Deep note path should exist");
        assert_eq!(deep_path, "Level1/Level2/Level3/Level4/Level5/deep.md");
    }

    #[test]
    fn test_get_folder_contents_recursive() {
        let db = setup_test_database();
        let user_id = "recursive_test_user";

        // Create nested folder structure:
        // Root/
        //   ├── Documents/
        //   │   ├── Projects/
        //   │   │   ├── project1.md
        //   │   │   └── SubProjects/
        //   │   │       └── subproject.txt
        //   │   └── notes.md
        //   ├── Work/
        //   │   └── agenda.org
        //   └── readme.md

        let root_id = db
            .create_folder("Root", None)
            .expect("Failed to create Root folder");
        let docs_id = db
            .create_folder("Documents", Some(&root_id))
            .expect("Failed to create Documents folder");
        let projects_id = db
            .create_folder("Projects", Some(&docs_id))
            .expect("Failed to create Projects folder");
        let subprojects_id = db
            .create_folder("SubProjects", Some(&projects_id))
            .expect("Failed to create SubProjects folder");
        let work_id = db
            .create_folder("Work", Some(&root_id))
            .expect("Failed to create Work folder");

        // Create notes
        db.create_note(
            "readme",
            "readme",
            None,
            "Root readme",
            "md",
            Some(&root_id),
            user_id,
        )
        .expect("Failed to create root readme");
        db.create_note(
            "notes",
            "notes",
            None,
            "Doc notes",
            "md",
            Some(&docs_id),
            user_id,
        )
        .expect("Failed to create docs notes");
        db.create_note(
            "project1",
            "project1",
            None,
            "Project content",
            "md",
            Some(&projects_id),
            user_id,
        )
        .expect("Failed to create project1");
        db.create_note(
            "subproject",
            "subproject",
            None,
            "Sub content",
            "txt",
            Some(&subprojects_id),
            user_id,
        )
        .expect("Failed to create subproject");
        db.create_note(
            "agenda",
            "agenda",
            None,
            "Work agenda",
            "org",
            Some(&work_id),
            user_id,
        )
        .expect("Failed to create work agenda");

        // Test getting all contents under root
        let contents = db
            .get_folder_contents_recursive(&root_id, user_id)
            .expect("Failed to get recursive contents");

        // Extract paths for easier testing
        let paths: Vec<String> = contents
            .iter()
            .map(|item| match item {
                FileType::Directory { path } => path.clone(),
                FileType::File { path } => path.clone(),
            })
            .collect();

        // Check that we have all expected items
        assert!(paths.contains(&"Documents".to_string()));
        assert!(paths.contains(&"Documents/Projects".to_string()));
        assert!(paths.contains(&"Documents/Projects/SubProjects".to_string()));
        assert!(paths.contains(&"Work".to_string()));
        assert!(paths.contains(&"readme.md".to_string()));
        assert!(paths.contains(&"Documents/notes.md".to_string()));
        assert!(paths.contains(&"Documents/Projects/project1.md".to_string()));
        assert!(paths.contains(&"Documents/Projects/SubProjects/subproject.txt".to_string()));
        assert!(paths.contains(&"Work/agenda.org".to_string()));

        // Check types are correct
        let directories: Vec<&String> = contents
            .iter()
            .filter_map(|item| match item {
                FileType::Directory { path } => Some(path),
                FileType::File { .. } => None,
            })
            .collect();

        let files: Vec<&String> = contents
            .iter()
            .filter_map(|item| match item {
                FileType::File { path } => Some(path),
                FileType::Directory { .. } => None,
            })
            .collect();

        assert_eq!(directories.len(), 4); // Documents, Projects, SubProjects, Work
        assert_eq!(files.len(), 5); // readme, notes, project1, subproject, agenda

        // Test getting contents under Documents folder only
        let docs_contents = db
            .get_folder_contents_recursive(&docs_id, user_id)
            .expect("Failed to get docs contents");

        let docs_paths: Vec<String> = docs_contents
            .iter()
            .map(|item| match item {
                FileType::Directory { path } => path.clone(),
                FileType::File { path } => path.clone(),
            })
            .collect();

        assert!(docs_paths.contains(&"Projects".to_string()));
        assert!(docs_paths.contains(&"Projects/SubProjects".to_string()));
        assert!(docs_paths.contains(&"notes.md".to_string()));
        assert!(docs_paths.contains(&"Projects/project1.md".to_string()));
        assert!(docs_paths.contains(&"Projects/SubProjects/subproject.txt".to_string()));

        // Should not contain Work or root items
        assert!(!docs_paths.contains(&"Work".to_string()));
        assert!(!docs_paths.contains(&"readme.md".to_string()));
        assert!(!docs_paths.contains(&"Work/agenda.org".to_string()));
    }

    #[test]
    fn test_get_folder_contents_recursive_empty_folder() {
        let db = setup_test_database();
        let user_id = "empty_test_user";

        // Create empty folder
        let empty_id = db
            .create_folder("Empty", None)
            .expect("Failed to create empty folder");

        let contents = db
            .get_folder_contents_recursive(&empty_id, user_id)
            .expect("Failed to get empty folder contents");

        assert_eq!(contents.len(), 0);
    }

    #[test]
    fn test_get_folder_contents_recursive_user_filtering() {
        let db = setup_test_database();
        let user1 = "user1";
        let user2 = "user2";

        // Create folder structure
        let shared_folder_id = db
            .create_folder("Shared", None)
            .expect("Failed to create shared folder");

        // Create notes for different users in same folder
        db.create_note(
            "note1",
            "note1",
            None,
            "User 1 content",
            "md",
            Some(&shared_folder_id),
            user1,
        )
        .expect("Failed to create user1 note");
        db.create_note(
            "note2",
            "note2",
            None,
            "User 2 content",
            "md",
            Some(&shared_folder_id),
            user2,
        )
        .expect("Failed to create user2 note");

        // Test that each user only sees their own notes
        let user1_contents = db
            .get_folder_contents_recursive(&shared_folder_id, user1)
            .expect("Failed to get user1 contents");
        let user2_contents = db
            .get_folder_contents_recursive(&shared_folder_id, user2)
            .expect("Failed to get user2 contents");

        assert_eq!(user1_contents.len(), 1);
        assert_eq!(user2_contents.len(), 1);

        // Check that user1 sees note1.md and user2 sees note2.md
        match &user1_contents[0] {
            FileType::File { path } => assert_eq!(path, "note1.md"),
            _ => panic!("Expected file, got directory"),
        }

        match &user2_contents[0] {
            FileType::File { path } => assert_eq!(path, "note2.md"),
            _ => panic!("Expected file, got directory"),
        }
    }

    #[test]
    fn test_get_folder_contents_recursive_single_level() {
        let db = setup_test_database();
        let user_id = "single_level_user";

        // Create folder with only direct children
        let folder_id = db
            .create_folder("SingleLevel", None)
            .expect("Failed to create folder");
        let child_folder_id = db
            .create_folder("ChildFolder", Some(&folder_id))
            .expect("Failed to create child folder");

        db.create_note(
            "file1",
            "file1",
            None,
            "Content 1",
            "txt",
            Some(&folder_id),
            user_id,
        )
        .expect("Failed to create file1");
        db.create_note(
            "file2",
            "file2",
            None,
            "Content 2",
            "md",
            Some(&folder_id),
            user_id,
        )
        .expect("Failed to create file2");

        let contents = db
            .get_folder_contents_recursive(&folder_id, user_id)
            .expect("Failed to get single level contents");

        assert_eq!(contents.len(), 3); // 1 folder + 2 files

        let paths: Vec<String> = contents
            .iter()
            .map(|item| match item {
                FileType::Directory { path } => path.clone(),
                FileType::File { path } => path.clone(),
            })
            .collect();

        assert!(paths.contains(&"ChildFolder".to_string()));
        assert!(paths.contains(&"file1.txt".to_string()));
        assert!(paths.contains(&"file2.md".to_string()));
    }

    #[test]
    fn test_get_child_count() {
        let db = setup_test_database();
        let user_id = "count_test_user";
        let other_user = "other_count_user";

        // Create folder structure
        let parent_folder_id = db
            .create_folder("Parent", None)
            .expect("Failed to create parent folder");
        let child_folder1_id = db
            .create_folder("Child1", Some(&parent_folder_id))
            .expect("Failed to create child folder 1");
        let child_folder2_id = db
            .create_folder("Child2", Some(&parent_folder_id))
            .expect("Failed to create child folder 2");

        // Create notes in parent folder for different users
        db.create_note(
            "note1",
            "Note 1",
            None,
            "Content 1",
            "md",
            Some(&parent_folder_id),
            user_id,
        )
        .expect("Failed to create note 1");
        db.create_note(
            "note2",
            "Note 2",
            None,
            "Content 2",
            "txt",
            Some(&parent_folder_id),
            user_id,
        )
        .expect("Failed to create note 2");
        db.create_note(
            "note3",
            "Note 3",
            None,
            "Content 3",
            "md",
            Some(&parent_folder_id),
            other_user,
        )
        .expect("Failed to create note 3 for other user");

        // Create notes at root level
        db.create_note("root1", "Root 1", None, "Root content", "md", None, user_id)
            .expect("Failed to create root note 1");
        db.create_note(
            "root2",
            "Root 2",
            None,
            "Root content",
            "org",
            None,
            other_user,
        )
        .expect("Failed to create root note 2 for other user");

        // Test counting children of parent folder with specific user
        let (folder_count, note_count) = db
            .get_child_count(Some(&parent_folder_id), Some(user_id))
            .expect("Failed to get child count for specific user");
        assert_eq!(folder_count, 2); // Child1, Child2
        assert_eq!(note_count, 2); // note1, note2 (only for user_id)

        // Test counting children of parent folder with other user
        let (folder_count, note_count) = db
            .get_child_count(Some(&parent_folder_id), Some(other_user))
            .expect("Failed to get child count for other user");
        assert_eq!(folder_count, 2); // Child1, Child2 (folders are shared)
        assert_eq!(note_count, 1); // note3 (only for other_user)

        // Test counting children of parent folder without user filter
        let (folder_count, note_count) = db
            .get_child_count(Some(&parent_folder_id), None)
            .expect("Failed to get child count without user filter");
        assert_eq!(folder_count, 2); // Child1, Child2
        assert_eq!(note_count, 3); // note1, note2, note3 (all notes)

        // Test counting children of empty folder
        let empty_folder_id = db
            .create_folder("Empty", None)
            .expect("Failed to create empty folder");
        let (folder_count, note_count) = db
            .get_child_count(Some(&empty_folder_id), Some(user_id))
            .expect("Failed to get empty folder child count");
        assert_eq!(folder_count, 0);
        assert_eq!(note_count, 0);

        // Test counting root level items with specific user
        let (folder_count, note_count) = db
            .get_child_count(None, Some(user_id))
            .expect("Failed to get root count for specific user");
        assert_eq!(folder_count, 2); // Parent, Empty (folders are not user-specific)
        assert_eq!(note_count, 1); // root1 (only for user_id)

        // Test counting root level items without user filter
        let (folder_count, note_count) = db
            .get_child_count(None, None)
            .expect("Failed to get root count without user filter");
        assert_eq!(folder_count, 2); // Parent, Empty
        assert_eq!(note_count, 2); // root1, root2 (all root notes)
    }

    #[test]
    fn test_get_child_count_edge_cases() {
        let db = setup_test_database();
        let user_id = "edge_test_user";

        // Test with non-existent folder ID
        let (folder_count, note_count) = db
            .get_child_count(Some("non_existent_id"), Some(user_id))
            .expect("Failed to get count for non-existent folder");
        assert_eq!(folder_count, 0);
        assert_eq!(note_count, 0);

        // Test with non-existent user ID
        let folder_id = db
            .create_folder("Test", None)
            .expect("Failed to create test folder");
        db.create_note(
            "test_note",
            "Test",
            None,
            "Content",
            "md",
            Some(&folder_id),
            user_id,
        )
        .expect("Failed to create test note");

        let (folder_count, note_count) = db
            .get_child_count(Some(&folder_id), Some("non_existent_user"))
            .expect("Failed to get count for non-existent user");
        assert_eq!(folder_count, 1); // Folders are not user-specific, so we created one above
        assert_eq!(note_count, 0); // No notes for this user

        // Test deeply nested structure
        let mut current_parent = Some(folder_id.clone());
        for i in 1..=3 {
            let child_id = db
                .create_folder(&format!("Level{}", i), current_parent.as_deref())
                .expect("Failed to create nested folder");
            current_parent = Some(child_id);
        }

        // The original folder should have 1 child (Level1)
        let (folder_count, note_count) = db
            .get_child_count(Some(&folder_id), Some(user_id))
            .expect("Failed to get count for nested structure");
        assert_eq!(folder_count, 1); // Level1
        assert_eq!(note_count, 1); // test_note
    }
}
