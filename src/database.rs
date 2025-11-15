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
}

#[derive(Debug, Clone)]
pub struct Folder {
    pub id: String,
    pub title: String,
    pub parent_id: Option<String>,
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

        Database::new(conn, Some(chrono_tz::US::Eastern))
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
}
