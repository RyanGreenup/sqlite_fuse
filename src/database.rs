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
            Some(_) => "SELECT id, title, parent_id, created_at, updated_at FROM folders WHERE parent_id = ?1 ORDER BY title",
            None => "SELECT id, title, parent_id, created_at, updated_at FROM folders WHERE parent_id IS NULL ORDER BY title"
        };

        let mut stmt = self.connection.prepare(query)?;
        let folder_iter = match parent_id {
            Some(pid) => stmt.query_map([pid], Self::map_folder_row)?,
            None => stmt.query_map([], Self::map_folder_row)?
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

    pub fn list_notes_by_parent(&self, parent_id: Option<&str>, user_id: &str) -> Result<Vec<Note>> {
        let query = match parent_id {
            Some(_) => "SELECT id, title, abstract, content, syntax, parent_id, user_id, created_at, updated_at
                       FROM notes WHERE parent_id = ?1 AND user_id = ?2 ORDER BY title",
            None => "SELECT id, title, abstract, content, syntax, parent_id, user_id, created_at, updated_at
                    FROM notes WHERE parent_id IS NULL AND user_id = ?1 ORDER BY title"
        };

        let mut stmt = self.connection.prepare(query)?;
        let note_iter = match parent_id {
            Some(pid) => stmt.query_map(params![pid, user_id], Self::map_note_row)?,
            None => stmt.query_map([user_id], Self::map_note_row)?
        };

        note_iter.collect()
    }

    /// Maps a database row to a Folder struct, handling datetime parsing.
    /// Extracted as a helper to avoid code duplication across query methods.
    fn map_folder_row(row: &rusqlite::Row) -> rusqlite::Result<Folder> {
        Ok(Folder {
            id: row.get(0)?,
            title: row.get(1)?,
            parent_id: row.get(2)?,
            created_at: NaiveDateTime::parse_from_str(&row.get::<_, String>(3)?, "%Y-%m-%d %H:%M:%S")
                .map_err(|_| rusqlite::Error::InvalidColumnType(3, "created_at".to_string(), rusqlite::types::Type::Text))?
                .and_utc(),
            updated_at: NaiveDateTime::parse_from_str(&row.get::<_, String>(4)?, "%Y-%m-%d %H:%M:%S")
                .map_err(|_| rusqlite::Error::InvalidColumnType(4, "updated_at".to_string(), rusqlite::types::Type::Text))?
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
            created_at: NaiveDateTime::parse_from_str(&row.get::<_, String>(7)?, "%Y-%m-%d %H:%M:%S")
                .map_err(|_| rusqlite::Error::InvalidColumnType(7, "created_at".to_string(), rusqlite::types::Type::Text))?
                .and_utc(),
            updated_at: NaiveDateTime::parse_from_str(&row.get::<_, String>(8)?, "%Y-%m-%d %H:%M:%S")
                .map_err(|_| rusqlite::Error::InvalidColumnType(8, "updated_at".to_string(), rusqlite::types::Type::Text))?
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
        let non_existent = db.get_note_by_id("non_existent_id")
            .expect("Failed to query non-existent note");
        assert!(non_existent.is_none());

        // Create a note directly in database for testing
        db.connection.execute(
            "INSERT INTO notes (id, title, abstract, content, syntax, parent_id, user_id, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
            params![note_id, "Test Note", "Test abstract", "# Test Content", "md", None::<String>, user_id, "2023-01-01 12:00:00"]
        ).expect("Failed to insert test note");

        // Test getting existing note
        let note = db.get_note_by_id(note_id)
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
        let note_id = db.create_note(
            "note_1",
            "My First Note",
            Some("An abstract"),
            "# Hello World",
            "md",
            None,
            user_id
        ).expect("Failed to create note");
        assert_eq!(note_id, "note_1");

        // Verify note was created
        let note = db.get_note_by_id("note_1")
            .expect("Failed to query created note")
            .expect("Created note not found");

        assert_eq!(note.title, "My First Note");
        assert_eq!(note.abstract_text, Some("An abstract".to_string()));
        assert_eq!(note.content, "# Hello World");
        assert_eq!(note.syntax, "md");
        assert_eq!(note.user_id, user_id);

        // Create note with parent folder
        let folder_id = db.create_folder("Notes Folder", None)
            .expect("Failed to create folder");

        let note_id2 = db.create_note(
            "note_2",
            "Second Note",
            None,
            "Some content",
            "txt",
            Some(&folder_id),
            user_id
        ).expect("Failed to create note with parent");

        let note2 = db.get_note_by_id(&note_id2)
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

        let note_id = db.create_note(
            "update_test",
            "Original Title",
            Some("Original abstract"),
            "Original content",
            "md",
            None,
            user_id
        ).expect("Failed to create note for update test");

        // Test updating all content fields
        let updated = db.update_note(
            &note_id,
            "Updated Title",
            Some("Updated abstract"),
            "Updated content",
            "rst"
        ).expect("Failed to update note");
        assert!(updated);

        let note = db.get_note_by_id(&note_id)
            .expect("Failed to query updated note")
            .expect("Updated note not found");

        assert_eq!(note.title, "Updated Title");
        assert_eq!(note.abstract_text, Some("Updated abstract".to_string()));
        assert_eq!(note.content, "Updated content");
        assert_eq!(note.syntax, "rst");

        // Test updating with None abstract
        let updated2 = db.update_note(
            &note_id,
            "Final Title",
            None,
            "Final content",
            "md"
        ).expect("Failed to update note with None abstract");
        assert!(updated2);

        let note2 = db.get_note_by_id(&note_id)
            .expect("Failed to query updated note")
            .expect("Updated note not found");

        assert_eq!(note2.abstract_text, None);

        // Test updating non-existent note
        let not_updated = db.update_note(
            "non_existent",
            "Title",
            None,
            "Content",
            "md"
        ).expect("Failed to update non-existent note");
        assert!(!not_updated);
    }

    #[test]
    fn test_update_note_parent() {
        let db = setup_test_database();
        let user_id = "test_user_parent";

        let note_id = db.create_note(
            "parent_test",
            "Test Note",
            None,
            "Content",
            "md",
            None,
            user_id
        ).expect("Failed to create note for parent test");

        // Create folders
        let folder1_id = db.create_folder("Folder 1", None)
            .expect("Failed to create folder 1");
        let folder2_id = db.create_folder("Folder 2", None)
            .expect("Failed to create folder 2");

        // Move note to folder 1
        let updated = db.update_note_parent(&note_id, Some(&folder1_id))
            .expect("Failed to update note parent");
        assert!(updated);

        let note = db.get_note_by_id(&note_id)
            .expect("Failed to query note")
            .expect("Note not found");
        assert_eq!(note.parent_id, Some(folder1_id.clone()));

        // Move note to folder 2
        let updated2 = db.update_note_parent(&note_id, Some(&folder2_id))
            .expect("Failed to update note parent to folder 2");
        assert!(updated2);

        let note2 = db.get_note_by_id(&note_id)
            .expect("Failed to query note")
            .expect("Note not found");
        assert_eq!(note2.parent_id, Some(folder2_id));

        // Move note to root (no parent)
        let updated3 = db.update_note_parent(&note_id, None)
            .expect("Failed to update note parent to None");
        assert!(updated3);

        let note3 = db.get_note_by_id(&note_id)
            .expect("Failed to query note")
            .expect("Note not found");
        assert_eq!(note3.parent_id, None);

        // Test updating non-existent note
        let not_updated = db.update_note_parent("non_existent", Some(&folder1_id))
            .expect("Failed to update non-existent note parent");
        assert!(!not_updated);
    }

    #[test]
    fn test_delete_note() {
        let db = setup_test_database();
        let user_id = "test_user_delete";

        let note_id = db.create_note(
            "delete_test",
            "Delete Me",
            Some("Abstract"),
            "Content to delete",
            "md",
            None,
            user_id
        ).expect("Failed to create note for delete test");

        // Verify note exists
        let note = db.get_note_by_id(&note_id)
            .expect("Failed to query note before delete")
            .expect("Note should exist before delete");
        assert_eq!(note.title, "Delete Me");

        // Delete note
        let deleted = db.delete_note(&note_id)
            .expect("Failed to delete note");
        assert!(deleted);

        // Verify note is gone
        let deleted_note = db.get_note_by_id(&note_id)
            .expect("Failed to query deleted note");
        assert!(deleted_note.is_none());

        // Verify history was created (check if history table has the note)
        let mut stmt = db.connection.prepare(
            "SELECT COUNT(*) FROM notes_history WHERE id = ?1 AND log_action = 'DELETE'"
        ).expect("Failed to prepare history query");
        let count: i64 = stmt.query_row([&note_id], |row| row.get(0))
            .expect("Failed to query history count");
        assert_eq!(count, 1, "Note should be in history after deletion");

        // Test deleting non-existent note
        let not_deleted = db.delete_note("non_existent")
            .expect("Failed to delete non-existent note");
        assert!(!not_deleted);
    }

    #[test]
    fn test_list_notes_by_parent() {
        let db = setup_test_database();
        let user_id = "test_user_list";

        // Create folder
        let folder_id = db.create_folder("Test Folder", None)
            .expect("Failed to create folder");

        // Create notes in folder
        db.create_note("note1", "Note 1", None, "Content 1", "md", Some(&folder_id), user_id)
            .expect("Failed to create note 1");
        db.create_note("note2", "Note 2", None, "Content 2", "txt", Some(&folder_id), user_id)
            .expect("Failed to create note 2");

        // Create note at root level
        db.create_note("note3", "Root Note", None, "Root content", "md", None, user_id)
            .expect("Failed to create root note");

        // Create note for different user in same folder
        db.create_note("note4", "Other User", None, "Content", "md", Some(&folder_id), "other_user")
            .expect("Failed to create note for other user");

        // Test listing notes in folder for specific user
        let folder_notes = db.list_notes_by_parent(Some(&folder_id), user_id)
            .expect("Failed to list notes in folder");
        assert_eq!(folder_notes.len(), 2);

        let titles: Vec<&str> = folder_notes.iter().map(|n| n.title.as_str()).collect();
        assert!(titles.contains(&"Note 1"));
        assert!(titles.contains(&"Note 2"));
        assert!(!titles.contains(&"Other User"));

        // Test listing root notes for user
        let root_notes = db.list_notes_by_parent(None, user_id)
            .expect("Failed to list root notes");
        assert_eq!(root_notes.len(), 1);
        assert_eq!(root_notes[0].title, "Root Note");

        // Test listing notes for user with no notes in folder
        let empty_folder_id = db.create_folder("Empty Folder", None)
            .expect("Failed to create empty folder");
        let empty_notes = db.list_notes_by_parent(Some(&empty_folder_id), user_id)
            .expect("Failed to list notes in empty folder");
        assert_eq!(empty_notes.len(), 0);
    }

    #[test]
    fn test_fts_triggers() {
        let db = setup_test_database();
        let user_id = "fts_test_user";

        // Create a note
        let note_id = db.create_note(
            "fts_test",
            "Searchable Note",
            Some("This is searchable"),
            "Content with keywords",
            "md",
            None,
            user_id
        ).expect("Failed to create note for FTS test");

        // Verify note was added to FTS table
        let mut stmt = db.connection.prepare(
            "SELECT COUNT(*) FROM notes_fts WHERE id = ?1"
        ).expect("Failed to prepare FTS query");
        let count: i64 = stmt.query_row([&note_id], |row| row.get(0))
            .expect("Failed to query FTS count");
        assert_eq!(count, 1, "Note should be in FTS table after creation");

        // Update the note and verify FTS is updated
        db.update_note(&note_id, "Updated Searchable", Some("Updated abstract"), "Updated content", "md")
            .expect("Failed to update note");

        let mut stmt = db.connection.prepare(
            "SELECT title FROM notes_fts WHERE id = ?1"
        ).expect("Failed to prepare FTS query");
        let title: String = stmt.query_row([&note_id], |row| row.get(0))
            .expect("Failed to query FTS title");
        assert_eq!(title, "Updated Searchable", "FTS should reflect updated title");

        // Delete the note and verify it's removed from FTS
        db.delete_note(&note_id).expect("Failed to delete note");

        let mut stmt = db.connection.prepare(
            "SELECT COUNT(*) FROM notes_fts WHERE id = ?1"
        ).expect("Failed to prepare FTS query");
        let count: i64 = stmt.query_row([&note_id], |row| row.get(0))
            .expect("Failed to query FTS count");
        assert_eq!(count, 0, "Note should be removed from FTS table after deletion");
    }
}
