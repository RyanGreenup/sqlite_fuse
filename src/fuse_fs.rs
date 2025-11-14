use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fuser::Filesystem;

use libc::ENOENT;
use std::ffi::OsStr;

use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request,
};

use crate::database::Database;

const TTL: Duration = Duration::from_secs(1); // 1 second

pub fn hello() {
    println!("Hello");
}

pub struct ExampleFuseFs {
    inode_map: HashMap<String, u64>,
    reverse_inode_map: HashMap<u64, String>,
    next_inode: u64,
    db: Database,
}

impl ExampleFuseFs {
    fn get_files(parent_id: &str) -> Vec<String> {
        return (1..5)
            .into_iter()
            .map(|v| format!("{parent_id}-{v}"))
            .collect();
    }

    fn is_dir(&self, path: &str) -> bool {
        let parent_id = self.get_parent_id_from_path(path);
        if let Some(parent_id) = parent_id {
            return self.db.get_child_count(&parent_id) > 0;
        } else {
            return false;
        }
    }

    fn content_size(&self, path: &str) -> usize {
        let id = self.db.get_id_from_path(path);
        if let Some(id) = id {
            match self.db.get_content(&id) {
                Some(s) => s.len(),
                None => 0,
            }
        } else {
            eprintln!("[WARNING] Unable to get id for {path}");
            return 0;
        }
    }

    // Unused
    /*
    fn get_dirs(parent_id: &str) -> Vec<String> {
        return vec!["A", "B", "C"]
            .into_iter()
            .map(|v| format!("{parent_id}-{v}"))
            .collect();
    }

    fn get_root_dirs() -> Vec<String> {
        return vec!["A".into(), "B".into(), "C".into()];
    }

    fn get_root_files() -> Vec<String> {
        return vec!["1".into(), "2".into(), "3".into()];
    }
    */

    fn get_path_from_inode(&self, inode: u64) -> Option<&String> {
        self.reverse_inode_map.get(&inode)
    }

    fn get_or_create_inode(&mut self, path: &str) -> u64 {
        if let Some(&inode) = self.inode_map.get(path) {
            return inode;
        }

        let inode = self.next_inode;
        self.next_inode += 1;
        self.inode_map.insert(path.to_string(), inode);
        self.reverse_inode_map.insert(inode, path.to_string());
        inode
    }

    pub fn get_parent_id_from_path(&self, path: &str) -> Option<String> {
        if let Some(id) = self.db.get_id_from_path(path) {
            eprintln!("Unable to get ID from {path}");
            if let Some(item) = self.db.get(&id) {
                return item.parent_id.clone();
            }
        }
        None
    }

    fn is_editor_temp_file(filename: &str) -> bool {
        // Vim/Neovim swap files
        if filename.starts_with('.') && filename.ends_with(".swp") {
            return true;
        }
        if filename.starts_with('.') && filename.ends_with(".swo") {
            return true;
        }
        if filename.starts_with('.') && filename.ends_with(".tmp") {
            return true;
        }

        // Vim backup files
        if filename.ends_with('~') {
            return true;
        }

        // Emacs backup and auto-save files
        if filename.starts_with('#') && filename.ends_with('#') {
            return true;
        }
        if filename.starts_with(".#") {
            return true;
        }

        // VSCode temporary files
        if filename.starts_with(".vscode") {
            return true;
        }

        // General temporary file patterns
        if filename.contains(".tmp.") || filename.ends_with(".tmp") {
            return true;
        }
        if filename.contains(".temp.") || filename.ends_with(".temp") {
            return true;
        }

        false
    }
    fn get_parent_id(&self, parent_path: &str) -> Option<String> {
        let parent_note_id = if parent_path == "/" {
            None
        } else {
            match self.get_parent_id_from_path(parent_path) {
                Some(id) => Some(id),
                None => return None,
            }
        };

        return parent_note_id;
    }
}

impl Filesystem for ExampleFuseFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Get parent path
        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Construct full path
        let full_path = if parent_path == "/" {
            format!("/{name_str}")
        } else {
            format!("{parent_path}/{name_str}")
        };

        // Is it a directory
        let is_dir = self.is_dir(&full_path);

        // Handle editor temporary files with synthetic attributes
        if Self::is_editor_temp_file(name_str) {
            let inode = self.get_or_create_inode(&full_path);

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let attr = FileAttr {
                ino: inode,
                size: 0,
                blocks: 0,
                atime: UNIX_EPOCH + Duration::from_secs(now),
                mtime: UNIX_EPOCH + Duration::from_secs(now),
                ctime: UNIX_EPOCH + Duration::from_secs(now),
                crtime: UNIX_EPOCH + Duration::from_secs(now),
                kind: FileType::RegularFile,
                perm: 0o644,
                nlink: 1,
                uid: 501,
                gid: 20,
                rdev: 0,
                flags: 0,
                blksize: 512,
            };

            reply.entry(&TTL, &attr, 0);
            return;
        }

        if is_dir {
            // This note has children, so it's a directory
            let inode = self.get_or_create_inode(&full_path);
            let attr = FileAttr {
                ino: inode,
                size: 0,
                blocks: 0,
                atime: UNIX_EPOCH,  // TODO: Parse created_at
                mtime: UNIX_EPOCH,  // TODO: Parse updated_at
                ctime: UNIX_EPOCH,  // TODO: Parse updated_at
                crtime: UNIX_EPOCH, // TODO: Parse created_at
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: 501,
                gid: 20,
                rdev: 0,
                flags: 0,
                blksize: 512,
            };
            reply.entry(&TTL, &attr, 0);
            return;
        } else {
            let inode = self.get_or_create_inode(&full_path);
            let content_size = self.content_size(&full_path) as u64;

            // TODO: Parse timestamp strings properly
            let attr = FileAttr {
                ino: inode,
                size: content_size,
                blocks: content_size.div_ceil(512) as u64,
                atime: UNIX_EPOCH,  // TODO: Parse created_at
                mtime: UNIX_EPOCH,  // TODO: Parse updated_at
                ctime: UNIX_EPOCH,  // TODO: Parse updated_at
                crtime: UNIX_EPOCH, // TODO: Parse created_at
                kind: FileType::RegularFile,
                perm: 0o644,
                nlink: 1,
                uid: 501,
                gid: 20,
                rdev: 0,
                flags: 0,
                blksize: 512,
            };
            reply.entry(&TTL, &attr, 0);
            return;
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if ino == 1 {
            // Root directory
            let attr = FileAttr {
                ino: 1,
                size: 0,
                blocks: 0,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: 501,
                gid: 20,
                rdev: 0,
                flags: 0,
                blksize: 512,
            };
            reply.attr(&TTL, &attr);
            return;
        }

        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Extract the filename and parent path
        let (parent_path, filename) = if let Some(pos) = path.rfind('/') {
            let parent = &path[..pos];
            let name = &path[pos + 1..];
            (if parent.is_empty() { "/" } else { parent }, name)
        } else {
            ("/", &path[..])
        };

        // Get parent note ID (None for root level)
        let parent_note_id = if parent_path == "/" {
            None
        } else {
            match self.get_parent_id_from_path(parent_path) {
                Some(id) => Some(id),
                None => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // is directory
        if self.is_dir(&path) {
            // This note has children, so it's a directory
            let attr = FileAttr {
                ino,
                size: 0,
                blocks: 0,
                atime: UNIX_EPOCH,  // TODO: Parse created_at
                mtime: UNIX_EPOCH,  // TODO: Parse updated_at
                ctime: UNIX_EPOCH,  // TODO: Parse updated_at
                crtime: UNIX_EPOCH, // TODO: Parse created_at
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: 501,
                gid: 20,
                rdev: 0,
                flags: 0,
                blksize: 512,
            };
            reply.attr(&TTL, &attr);
            return;
        } else {
            let attr = FileAttr {
                ino,
                size: self.content_size(&path) as u64,
                blocks: self.content_size(&path).div_ceil(512) as u64,
                atime: UNIX_EPOCH,  // TODO: Parse created_at
                mtime: UNIX_EPOCH,  // TODO: Parse updated_at
                ctime: UNIX_EPOCH,  // TODO: Parse updated_at
                crtime: UNIX_EPOCH, // TODO: Parse created_at
                kind: FileType::RegularFile,
                perm: 0o644,
                nlink: 1,
                uid: 501,
                gid: 20,
                rdev: 0,
                flags: 0,
                blksize: 512,
            };

            reply.attr(&TTL, &attr);
            return;
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        _size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Extract the filename and parent path
        let (parent_path, filename) = if let Some(pos) = path.rfind('/') {
            let parent = &path[..pos];
            let name = &path[pos + 1..];
            (if parent.is_empty() { "/" } else { parent }, name)
        } else {
            ("/", &path[..])
        };

        // Get parent note ID (None for root level)
        //
        let parent_note_id = if parent_path == "/" {
            None
        } else {
            match self.get_parent_folder_id(parent_path) {
                Ok(id) => Some(id),
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // Handle special case for index files (index.{ext})
        if filename.starts_with("index.") {
            if let Some(parent_id) = &parent_note_id {
                // Look up the parent note to get its content via index file
                if let Ok(content) = self.db.query_row(
                    "SELECT content, syntax FROM notes WHERE id = ?1",
                    [parent_id],
                    |row| {
                        let content: String = row.get(0)?;
                        let syntax: String = row.get(1)?;
                        Ok((content, syntax))
                    },
                ) {
                    let expected_ext = &content.1;
                    let expected_index = format!("index.{}", expected_ext);

                    if filename == expected_index {
                        let content_bytes = content.0.as_bytes();
                        let start = offset as usize;
                        if start < content_bytes.len() {
                            reply.data(&content_bytes[start..]);
                        } else {
                            reply.data(&[]);
                        }
                        return;
                    }
                }
            }
        }

        // Query database for note content (unified schema)
        let note_query = "SELECT id, content, syntax FROM notes WHERE parent_id IS ?1 AND title = ?2 ORDER BY updated_at DESC LIMIT 1";

        // Try stripping file extension and matching title (for files)
        if let Some(dot_pos) = filename.rfind('.') {
            let title_without_ext = &filename[..dot_pos];
            let requested_ext = &filename[dot_pos + 1..];

            if let Ok(note_result) = self.db.query_row(
                note_query,
                rusqlite::params![parent_note_id, title_without_ext],
                |row| {
                    let id: String = row.get(0)?;
                    let content: String = row.get(1)?;
                    let syntax: String = row.get(2)?;
                    Ok((id, content, syntax))
                },
            ) {
                // Check if this note has children (should be a file for reading)
                let has_children = self
                    .db
                    .query_row(
                        "SELECT COUNT(*) FROM notes WHERE parent_id = ?1",
                        [&note_result.0],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap_or(0)
                    > 0;

                if !has_children {
                    // This note has no children, so it's a file
                    // Verify the extension matches the syntax
                    let expected_ext = &note_result.2;
                    if requested_ext == expected_ext {
                        let content_bytes = note_result.1.as_bytes();
                        let start = offset as usize;
                        if start < content_bytes.len() {
                            reply.data(&content_bytes[start..]);
                        } else {
                            reply.data(&[]);
                        }
                        return;
                    }
                }
            }
        }

        reply.error(ENOENT);
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let mut entries = vec![
            (ino, FileType::Directory, ".".to_string()),
            (1, FileType::Directory, "..".to_string()),
        ];

        // Get the note ID for this directory (unified schema)
        let current_note_id = if path == "/" {
            None
        } else {
            match self.get_parent_folder_id(&path) {
                Ok(id) => Some(id),
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // Query all child notes under this directory
        let child_query =
            "SELECT id, title, syntax FROM notes WHERE parent_id IS ?1 ORDER BY updated_at DESC";

        // First, collect all the child note data
        let child_notes: Vec<(String, String, String)> = {
            if let Ok(mut stmt) = self.db.prepare(child_query) {
                if let Ok(rows) = stmt.query_map([current_note_id.as_deref()], |row| {
                    let id: String = row.get(0)?;
                    let title: String = row.get(1)?;
                    let syntax: String = row.get(2)?;
                    Ok((id, title, syntax))
                }) {
                    rows.filter_map(|row| row.ok()).collect()
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            }
        };

        // Now process the collected data without holding database borrows
        for (note_id, title, syntax) in child_notes {
            // Check if this note has children to determine if it's a directory
            let has_children = self
                .db
                .query_row(
                    "SELECT COUNT(*) FROM notes WHERE parent_id = ?1",
                    [&note_id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap_or(0)
                > 0;

            if has_children {
                // This note has children, so it's a directory
                let full_path = if path == "/" {
                    format!("/{title}")
                } else {
                    format!("{path}/{title}")
                };
                let inode = self.get_or_create_inode(&full_path);
                entries.push((inode, FileType::Directory, title.clone()));

                // Pre-create inode for index file (for future access when subdirectory is listed)
                let extension = &syntax;
                let index_filename = format!("index.{}", extension);
                let index_path = if path == "/" {
                    format!("/{title}/{index_filename}")
                } else {
                    format!("{path}/{title}/{index_filename}")
                };
                let _index_inode = self.get_or_create_inode(&index_path);
                // Note: We don't add the index file to the current directory listing
                // It will be shown when the subdirectory is listed
            } else {
                // This note has no children, so it's a file
                let extension = &syntax;
                let filename = format!("{}.{}", title, extension);
                let full_path = if path == "/" {
                    format!("/{filename}")
                } else {
                    format!("{path}/{filename}")
                };
                let inode = self.get_or_create_inode(&full_path);
                entries.push((inode, FileType::RegularFile, filename));
            }
        }

        // If this directory corresponds to a note with content, add the index file
        if let Some(note_id) = &current_note_id {
            if let Ok(note_info) = self.db.query_row(
                "SELECT title, syntax, content FROM notes WHERE id = ?1",
                [note_id],
                |row| {
                    let title: String = row.get(0)?;
                    let syntax: String = row.get(1)?;
                    let content: String = row.get(2)?;
                    Ok((title, syntax, content))
                },
            ) {
                // Only add index file if the note has content
                if !note_info.2.is_empty() {
                    let extension = &note_info.1;
                    let index_filename = format!("index.{}", extension);
                    let index_path = if path == "/" {
                        format!("/{index_filename}")
                    } else {
                        format!("{path}/{index_filename}")
                    };
                    let index_inode = self.get_or_create_inode(&index_path);
                    entries.push((index_inode, FileType::RegularFile, index_filename));
                }
            }
        }

        // Handle path conflicts - if there are duplicate titles, favor the most recent based on updated_at
        let mut seen_titles = std::collections::HashSet::new();
        let mut unique_entries = Vec::new();

        for entry in entries {
            if entry.2 == "." || entry.2 == ".." {
                unique_entries.push(entry);
            } else if !seen_titles.contains(&entry.2) {
                seen_titles.insert(entry.2.clone());
                unique_entries.push(entry);
            }
        }

        for (i, entry) in unique_entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(entry.0, (i + 1) as i64, entry.1, &entry.2) {
                break;
            }
        }
        reply.ok();
    }

    /// Handle directory creation operations (unified schema)
    ///
    /// In the unified schema, creating a directory means creating a note that will act as a folder.
    /// The note is created with empty content initially, and if children are added later,
    /// its content becomes accessible via index.{ext}.
    ///
    /// Key behaviors:
    /// - Creates a note in the database that represents a directory
    /// - Uses default syntax "markdown" for new directories
    /// - TODO: Need user_id - for now using placeholder
    fn mkdir(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let folder_name = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        // Get parent path
        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Get or create the note for this folder
        let _note_id = match self.get_or_create_note(&parent_path, folder_name, "", "md") {
            Ok(id) => id,
            Err(_) => {
                reply.error(libc::EIO);
                return;
            }
        };

        // Use the note_id (either existing or newly created) for further operations
        {
            // Create the full path for the new directory
            let full_path = if parent_path == "/" {
                format!("/{folder_name}")
            } else {
                format!("{parent_path}/{folder_name}")
            };

            // Create inode for the new directory
            let inode = self.get_or_create_inode(&full_path);

            // Get current timestamp for attributes
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let attr = FileAttr {
                ino: inode,
                size: 0,
                blocks: 0,
                atime: UNIX_EPOCH + Duration::from_secs(now),
                mtime: UNIX_EPOCH + Duration::from_secs(now),
                ctime: UNIX_EPOCH + Duration::from_secs(now),
                crtime: UNIX_EPOCH + Duration::from_secs(now),
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: 501,
                gid: 20,
                rdev: 0,
                flags: 0,
                blksize: 512,
            };

            reply.entry(&TTL, &attr, 0);
        }
    }

    /// Handle file creation operations (unified schema)
    ///
    /// Creates a new note in the database. The file extension determines the syntax type.
    /// In the unified schema, this creates a note that will be presented as a file until
    /// it gets children (at which point it becomes a directory with index.{ext} for content).
    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let file_name = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        // Get parent path
        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Handle editor temporary files by creating them as regular empty files
        // but don't store them in the database
        if Self::is_editor_temp_file(file_name) {
            // Create a temporary inode for editor files but don't persist to database
            let full_path = if parent_path == "/" {
                format!("/{file_name}")
            } else {
                format!("{parent_path}/{file_name}")
            };

            let inode = self.get_or_create_inode(&full_path);

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let attr = FileAttr {
                ino: inode,
                size: 0,
                blocks: 0,
                atime: UNIX_EPOCH + Duration::from_secs(now),
                mtime: UNIX_EPOCH + Duration::from_secs(now),
                ctime: UNIX_EPOCH + Duration::from_secs(now),
                crtime: UNIX_EPOCH + Duration::from_secs(now),
                kind: FileType::RegularFile,
                perm: 0o644,
                nlink: 1,
                uid: 501,
                gid: 20,
                rdev: 0,
                flags: 0,
                blksize: 512,
            };

            reply.created(&TTL, &attr, 0, inode, 0);
            return;
        }

        // Extract title and extension from filename
        let (title, extension) = if let Some(dot_pos) = file_name.rfind('.') {
            let title = &file_name[..dot_pos];
            let ext = Self::get_extension_from_filename(file_name);
            (title, ext)
        } else {
            // No extension, default to txt
            (file_name, "txt")
        };

        // Get or create the note for this file
        let _note_id = match self.get_or_create_note(&parent_path, title, "", extension) {
            Ok(id) => id,
            Err(_) => {
                reply.error(libc::EIO);
                return;
            }
        };

        // Use the note_id (either existing or newly created) for further operations
        {
            // Create the full path for the new file
            let full_path = if parent_path == "/" {
                format!("/{file_name}")
            } else {
                format!("{parent_path}/{file_name}")
            };

            // Create inode for the new file
            let inode = self.get_or_create_inode(&full_path);

            // Get current timestamp for attributes
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let attr = FileAttr {
                ino: inode,
                size: 0, // Empty file initially
                blocks: 0,
                atime: UNIX_EPOCH + Duration::from_secs(now),
                mtime: UNIX_EPOCH + Duration::from_secs(now),
                ctime: UNIX_EPOCH + Duration::from_secs(now),
                crtime: UNIX_EPOCH + Duration::from_secs(now),
                kind: FileType::RegularFile,
                perm: 0o644,
                nlink: 1,
                uid: 501,
                gid: 20,
                rdev: 0,
                flags: 0,
                blksize: 512,
            };

            // Return the created file with a file handle (using inode as fh)
            reply.created(&TTL, &attr, 0, inode, 0);
        }
    }

    /// Handle file write operations (unified schema)
    ///
    /// This method handles writing to both regular files and index files in the unified schema.
    /// The content is immediately written to the database's 'content' field.
    ///
    /// Key behaviors:
    /// - offset 0: Completely overwrites existing content
    /// - offset > 0: Inserts/appends data at the specified position
    /// - Updates timestamps (updated_at) in database
    /// - Supports writing to index files (`index.{ext}`) which write to the parent note's content
    /// - Supports writing to regular files (`{title}.{ext}`) which are leaf notes
    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Extract the filename and parent path
        let (parent_path, filename) = if let Some(pos) = path.rfind('/') {
            let parent = &path[..pos];
            let name = &path[pos + 1..];
            (if parent.is_empty() { "/" } else { parent }, name)
        } else {
            ("/", &path[..])
        };

        // Get the parent note ID (unified schema)
        let parent_note_id = if parent_path == "/" {
            None
        } else {
            match self.get_parent_folder_id(parent_path) {
                Ok(id) => Some(id),
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // Handle index file writes (writing to parent note's content)
        if filename.starts_with("index.") {
            if let Some(parent_id) = &parent_note_id {
                // Get current content of the parent note
                let current_content = match self.db.query_row(
                    "SELECT content FROM notes WHERE id = ?1",
                    [parent_id],
                    |row| row.get::<_, String>(0),
                ) {
                    Ok(content) => content,
                    Err(_) => {
                        reply.error(ENOENT);
                        return;
                    }
                };

                // Handle the write operation
                let new_content = if offset == 0 {
                    // Overwrite from the beginning
                    String::from_utf8_lossy(data).to_string()
                } else {
                    // Append or insert at offset
                    let mut content_bytes = current_content.into_bytes();
                    let start_pos = offset as usize;

                    if start_pos > content_bytes.len() {
                        // If offset is beyond current content, pad with zeros
                        content_bytes.resize(start_pos, 0);
                    }

                    // Replace or extend content
                    if start_pos + data.len() <= content_bytes.len() {
                        // Replace existing content
                        content_bytes[start_pos..start_pos + data.len()].copy_from_slice(data);
                    } else {
                        // Extend content
                        content_bytes.truncate(start_pos);
                        content_bytes.extend_from_slice(data);
                    }

                    String::from_utf8_lossy(&content_bytes).to_string()
                };

                // Update the parent note's content
                let now = Self::current_timestamp();
                match self.db.execute(
                    "UPDATE notes SET content = ?1, updated_at = ?2 WHERE id = ?3",
                    rusqlite::params![&new_content, &now, parent_id],
                ) {
                    Ok(_) => {
                        reply.written(data.len() as u32);
                    }
                    Err(_) => {
                        reply.error(libc::EIO);
                    }
                }
                return;
            }
        }

        // Handle regular file writes (writing to leaf note's content)
        if let Some(dot_pos) = filename.rfind('.') {
            let title_without_ext = &filename[..dot_pos];
            let requested_ext = &filename[dot_pos + 1..];

            // Look up the note by title (without extension)
            let note_query = "SELECT id, content, syntax FROM notes WHERE parent_id IS ?1 AND title = ?2 ORDER BY updated_at DESC LIMIT 1";

            if let Ok(note_result) = self.db.query_row(
                note_query,
                rusqlite::params![parent_note_id, title_without_ext],
                |row| {
                    let id: String = row.get(0)?;
                    let content: String = row.get(1)?;
                    let syntax: String = row.get(2)?;
                    Ok((id, content, syntax))
                },
            ) {
                // Verify the extension matches the note's syntax
                let expected_ext = &note_result.2;
                if requested_ext != expected_ext {
                    reply.error(ENOENT);
                    return;
                }

                // Check that this note has no children (is a file, not a directory)
                let has_children = self
                    .db
                    .query_row(
                        "SELECT COUNT(*) FROM notes WHERE parent_id = ?1",
                        [&note_result.0],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap_or(0)
                    > 0;

                if has_children {
                    // This note has children, so it's a directory - can't write to it directly
                    // User should write to the index file instead
                    reply.error(libc::EISDIR);
                    return;
                }

                // Handle the write operation
                let new_content = if offset == 0 {
                    // Overwrite from the beginning
                    String::from_utf8_lossy(data).to_string()
                } else {
                    // Append or insert at offset
                    let mut content_bytes = note_result.1.into_bytes();
                    let start_pos = offset as usize;

                    if start_pos > content_bytes.len() {
                        // If offset is beyond current content, pad with zeros
                        content_bytes.resize(start_pos, 0);
                    }

                    // Replace or extend content
                    if start_pos + data.len() <= content_bytes.len() {
                        // Replace existing content
                        content_bytes[start_pos..start_pos + data.len()].copy_from_slice(data);
                    } else {
                        // Extend content
                        content_bytes.truncate(start_pos);
                        content_bytes.extend_from_slice(data);
                    }

                    String::from_utf8_lossy(&content_bytes).to_string()
                };

                // Update the note's content
                let now = Self::current_timestamp();
                match self.db.execute(
                    "UPDATE notes SET content = ?1, updated_at = ?2 WHERE id = ?3",
                    rusqlite::params![&new_content, &now, &note_result.0],
                ) {
                    Ok(_) => {
                        reply.written(data.len() as u32);
                    }
                    Err(_) => {
                        reply.error(libc::EIO);
                    }
                }
                return;
            }
        }

        reply.error(ENOENT);
    }

    /// Handle file opening operations (unified schema)
    ///
    /// This method verifies that a file exists before allowing it to be opened.
    /// In the unified schema, this handles both regular files (leaf notes) and
    /// index files (content of parent notes that have children).
    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        // Verify that the inode exists and corresponds to a valid file
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Extract the filename and parent path for database verification
        let (parent_path, filename) = if let Some(pos) = path.rfind('/') {
            let parent = &path[..pos];
            let name = &path[pos + 1..];
            (if parent.is_empty() { "/" } else { parent }, name)
        } else {
            ("/", &path[..])
        };

        // Get parent note ID (None for root level)
        let parent_note_id = if parent_path == "/" {
            None
        } else {
            match self.get_parent_folder_id(parent_path) {
                Ok(id) => Some(id),
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // Handle special case for index files (index.{ext})
        if filename.starts_with("index.") {
            if let Some(parent_id) = &parent_note_id {
                // Check if the parent note exists and verify extension matches syntax
                let note_exists = self
                    .db
                    .query_row(
                        "SELECT syntax FROM notes WHERE id = ?1",
                        [parent_id],
                        |row| {
                            let syntax: String = row.get(0)?;
                            let expected_ext = &syntax;
                            let expected_index = format!("index.{}", expected_ext);
                            Ok(filename == expected_index)
                        },
                    )
                    .unwrap_or(false);

                if note_exists {
                    reply.opened(ino, 0);
                } else {
                    reply.error(ENOENT);
                }
                return;
            } else {
                // Index file at root level doesn't make sense
                reply.error(ENOENT);
                return;
            }
        }

        // Handle regular files - extract title and verify extension
        let (title, requested_ext) = if let Some(dot_pos) = filename.rfind('.') {
            let title = &filename[..dot_pos];
            let ext = &filename[dot_pos + 1..];
            (title, Some(ext))
        } else {
            // No extension - this shouldn't happen for files in our schema
            (filename, None)
        };

        // Look up the note in the database
        let note_query = "SELECT id, syntax FROM notes WHERE parent_id IS ?1 AND title = ?2 ORDER BY updated_at DESC LIMIT 1";

        if let Ok((note_id, syntax)) = self.db.query_row(
            note_query,
            rusqlite::params![parent_note_id, title],
            |row| {
                let id: String = row.get(0)?;
                let syntax: String = row.get(1)?;
                Ok((id, syntax))
            },
        ) {
            // Verify the file extension matches the note's syntax
            if let Some(req_ext) = requested_ext {
                let expected_ext = &syntax;
                if req_ext != expected_ext {
                    reply.error(ENOENT);
                    return;
                }
            }

            // Check if this note has children (making it a directory)
            let has_children = self
                .db
                .query_row(
                    "SELECT COUNT(*) FROM notes WHERE parent_id = ?1",
                    [&note_id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap_or(0)
                > 0;

            if has_children {
                // This note has children, so it should be accessed as a directory, not a file
                reply.error(ENOENT);
            } else {
                // This is a leaf note (file), allow opening
                reply.opened(ino, 0);
            }
        } else {
            // Note doesn't exist in database
            reply.error(ENOENT);
        }
    }

    /// Handle file attribute setting operations (unified schema)
    ///
    /// In the unified schema, this handles both regular files and index files.
    /// Index files (index.{ext}) provide access to parent note content when
    /// a note has children and becomes a directory.
    ///
    /// Key behaviors:
    /// - Handles size changes (truncation/extension of file content)
    /// - Supports both regular files ({title}.{ext}) and index files (index.{ext})
    /// - Updates timestamps in the database when modified
    /// - Validates that the file exists before making changes
    /// - Returns updated file attributes after successful changes
    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        // Get the file path from inode
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Extract the filename and parent path for database operations
        let (parent_path, filename) = if let Some(pos) = path.rfind('/') {
            let parent = &path[..pos];
            let name = &path[pos + 1..];
            (if parent.is_empty() { "/" } else { parent }, name)
        } else {
            ("/", &path[..])
        };

        // Handle index files (index.{ext}) - these access parent note content
        if filename.starts_with("index.") {
            let parent_note_id = if parent_path == "/" {
                reply.error(ENOENT); // Root can't have index file
                return;
            } else {
                match self.get_parent_folder_id(parent_path) {
                    Ok(id) => id,
                    Err(_) => {
                        reply.error(ENOENT);
                        return;
                    }
                }
            };

            // Handle size changes for index file (modifies parent note content)
            if let Some(new_size) = size {
                let current_content = match self.db.query_row(
                    "SELECT content FROM notes WHERE id = ?1",
                    [&parent_note_id],
                    |row| row.get::<_, String>(0),
                ) {
                    Ok(content) => content,
                    Err(_) => {
                        reply.error(ENOENT);
                        return;
                    }
                };

                let mut content_bytes = current_content.into_bytes();
                let target_size = new_size as usize;

                // Adjust content size based on target
                if target_size < content_bytes.len() {
                    content_bytes.truncate(target_size);
                } else if target_size > content_bytes.len() {
                    content_bytes.resize(target_size, 0);
                }

                let new_content = String::from_utf8_lossy(&content_bytes).to_string();

                // Update parent note content
                let now = Self::current_timestamp();
                if let Err(_) = self.db.execute(
                    "UPDATE notes SET content = ?1, updated_at = ?2 WHERE id = ?3",
                    [&new_content, &now, &parent_note_id],
                ) {
                    reply.error(libc::EIO);
                    return;
                }
            }

            // Get current parent note information for returning attributes
            let (content_size, _created_at, _updated_at) = match self.db.query_row(
                "SELECT content, created_at, updated_at FROM notes WHERE id = ?1",
                [&parent_note_id],
                |row| {
                    let content: String = row.get(0)?;
                    let created: String = row.get(1)?;
                    let updated: String = row.get(2)?;
                    Ok((content.len(), created, updated))
                },
            ) {
                Ok(data) => data,
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            };

            let attr = FileAttr {
                ino,
                size: content_size as u64,
                blocks: content_size.div_ceil(512) as u64,
                atime: UNIX_EPOCH,  // TODO: Parse created_at string
                mtime: UNIX_EPOCH,  // TODO: Parse updated_at string
                ctime: UNIX_EPOCH,  // TODO: Parse updated_at string
                crtime: UNIX_EPOCH, // TODO: Parse created_at string
                kind: FileType::RegularFile,
                perm: mode.unwrap_or(0o644) as u16,
                nlink: 1,
                uid: uid.unwrap_or(501),
                gid: gid.unwrap_or(20),
                rdev: 0,
                flags: 0,
                blksize: 512,
            };

            reply.attr(&TTL, &attr);
            return;
        }

        // Handle regular files - strip extension and find note by title
        let parent_note_id = if parent_path == "/" {
            None
        } else {
            match self.get_parent_folder_id(parent_path) {
                Ok(id) => Some(id),
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // Extract title from filename (remove extension)
        let title = if let Some(dot_pos) = filename.rfind('.') {
            &filename[..dot_pos]
        } else {
            filename
        };

        // Handle size changes (file truncation/extension)
        if let Some(new_size) = size {
            let current_content = match self.db.query_row(
                "SELECT content FROM notes WHERE parent_id IS ?1 AND title = ?2 ORDER BY updated_at DESC LIMIT 1",
                rusqlite::params![parent_note_id, title],
                |row| row.get::<_, String>(0)
            ) {
                Ok(content) => content,
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            };

            let mut content_bytes = current_content.into_bytes();
            let target_size = new_size as usize;

            // Adjust content size based on target
            if target_size < content_bytes.len() {
                content_bytes.truncate(target_size);
            } else if target_size > content_bytes.len() {
                content_bytes.resize(target_size, 0);
            }

            let new_content = String::from_utf8_lossy(&content_bytes).to_string();

            // Update content in database
            let now = Self::current_timestamp();
            if let Err(_) = self.db.execute(
                "UPDATE notes SET content = ?1, updated_at = ?2 WHERE parent_id IS ?3 AND title = ?4",
                rusqlite::params![&new_content, &now, parent_note_id, title],
            ) {
                reply.error(libc::EIO);
                return;
            }
        }

        // Get current file information for returning updated attributes
        let (content_size, _created_at, _updated_at) = match self.db.query_row(
            "SELECT content, created_at, updated_at FROM notes WHERE parent_id IS ?1 AND title = ?2 ORDER BY updated_at DESC LIMIT 1",
            rusqlite::params![parent_note_id, title],
            |row| {
                let content: String = row.get(0)?;
                let created: String = row.get(1)?;
                let updated: String = row.get(2)?;
                Ok((content.len(), created, updated))
            }
        ) {
            Ok(data) => data,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        // Return updated file attributes
        let attr = FileAttr {
            ino,
            size: content_size as u64,
            blocks: content_size.div_ceil(512) as u64,
            atime: UNIX_EPOCH,  // TODO: Parse created_at string
            mtime: UNIX_EPOCH,  // TODO: Parse updated_at string
            ctime: UNIX_EPOCH,  // TODO: Parse updated_at string
            crtime: UNIX_EPOCH, // TODO: Parse created_at string
            kind: FileType::RegularFile,
            perm: mode.unwrap_or(0o644) as u16,
            nlink: 1,
            uid: uid.unwrap_or(501),
            gid: gid.unwrap_or(20),
            rdev: 0,
            flags: 0,
            blksize: 512,
        };

        reply.attr(&TTL, &attr);
    }

    /// Handle file flush operations
    /// This method is called when editors or applications want to ensure that
    /// all pending writes have been completed. Since we write directly to the
    /// database in our write() method, this is essentially a no-op, but we
    /// need to implement it for editor compatibility.
    ///
    /// Key behaviors:
    /// - Always returns success since writes are already persistent
    /// - Required for proper editor functionality (many editors call flush before close)
    /// - Validates that the file handle corresponds to a valid file
    fn flush(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: fuser::ReplyEmpty,
    ) {
        // Verify that the inode exists (basic validation)
        if self.get_path_from_inode(ino).is_some() {
            // Since we write directly to the database, flush is always successful
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    /// Handle file release (close) operations
    /// This method is called when a file handle is closed. Since we don't
    /// maintain any file-specific state or resources, this is essentially
    /// a no-op, but it's required for proper FUSE operation.
    ///
    /// Key behaviors:
    /// - Always returns success since no cleanup is needed
    /// - Called when editors close files or when file handles are released
    /// - Validates that the file handle corresponds to a valid file
    fn release(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        // Verify that the inode exists (basic validation)
        if self.get_path_from_inode(ino).is_some() {
            // No cleanup needed since we don't maintain file-specific resources
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    /// Handle file and directory renaming operations (unified schema)
    ///
    /// In the unified schema, renaming works on notes regardless of whether they're
    /// currently presented as files or directories. The operation handles:
    /// - Title changes (with proper extension handling)
    /// - Moving between directories (parent_id changes)
    /// - Updating timestamps
    ///
    /// Key behaviors:
    /// - Single table operation (notes table only)
    /// - Handles both file and directory renaming automatically
    /// - Strips extensions when storing titles in database
    /// - Updates inode mappings for renamed items and their descendants
    /// - Proper NULL handling for parent_id
    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        let old_name = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let new_name = match newname.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        // Get parent paths
        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let new_parent_path = match self.get_path_from_inode(newparent) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Get parent note IDs from database (None for root level)
        let parent_note_id = if parent_path == "/" {
            None
        } else {
            match self.get_parent_folder_id(&parent_path) {
                Ok(id) => Some(id),
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        let new_parent_note_id = if new_parent_path == "/" {
            None
        } else {
            match self.get_parent_folder_id(&new_parent_path) {
                Ok(id) => Some(id),
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // Handle special case for index files (index.{ext})
        if old_name.starts_with("index.") && new_name.starts_with("index.") {
            // Renaming index file - this changes the parent note's syntax
            if let Some(parent_id) = &parent_note_id {
                if let Some(new_parent_id) = &new_parent_note_id {
                    if parent_id == new_parent_id {
                        // Same parent, just changing syntax
                        if let Some(new_dot_pos) = new_name.rfind('.') {
                            let new_ext = &new_name[new_dot_pos + 1..];
                            let new_extension = Self::normalize_extension(new_ext);

                            // Get current timestamp in Australia/Sydney timezone
                            let now = Self::current_timestamp();

                            let update_result = self.db.execute(
                                "UPDATE notes SET syntax = ?1, updated_at = ?2 WHERE id = ?3",
                                rusqlite::params![new_extension, &now, parent_id],
                            );

                            match update_result {
                                Ok(rows_affected) => {
                                    if rows_affected > 0 {
                                        // Update inode mappings
                                        let old_path = if parent_path == "/" {
                                            format!("/{old_name}")
                                        } else {
                                            format!("{parent_path}/{old_name}")
                                        };
                                        let new_path = if new_parent_path == "/" {
                                            format!("/{new_name}")
                                        } else {
                                            format!("{new_parent_path}/{new_name}")
                                        };

                                        if let Some(inode) = self.inode_map.remove(&old_path) {
                                            self.inode_map.insert(new_path.clone(), inode);
                                            self.reverse_inode_map.insert(inode, new_path);
                                        }

                                        reply.ok();
                                        return;
                                    }
                                }
                                Err(_) => {
                                    reply.error(libc::EIO);
                                    return;
                                }
                            }
                        }
                    }
                }
            }
            reply.error(ENOENT);
            return;
        }

        // Handle regular file/directory renaming
        // Extract titles and extensions
        let (old_title, old_ext) = if let Some(dot_pos) = old_name.rfind('.') {
            (&old_name[..dot_pos], Some(&old_name[dot_pos + 1..]))
        } else {
            (old_name, None)
        };

        let (new_title, new_ext) = if let Some(dot_pos) = new_name.rfind('.') {
            (&new_name[..dot_pos], Some(&new_name[dot_pos + 1..]))
        } else {
            (new_name, None)
        };

        // Find the note to rename and get its current syntax
        let note_query = "SELECT id, syntax FROM notes WHERE parent_id IS ?1 AND title = ?2 ORDER BY updated_at DESC LIMIT 1";

        let note_result = self.db.query_row(
            note_query,
            rusqlite::params![parent_note_id, old_title],
            |row| {
                let id: String = row.get(0)?;
                let syntax: String = row.get(1)?;
                Ok((id, syntax))
            },
        );

        let (note_id, current_syntax) = match note_result {
            Ok(result) => result,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        // Validate old extension matches current syntax (if file has extension)
        if let Some(old_extension) = old_ext {
            let expected_ext = &current_syntax;
            if old_extension != expected_ext {
                reply.error(ENOENT);
                return;
            }
        }

        // Check if this note has children to determine if it's a directory
        let has_children = self
            .db
            .query_row(
                "SELECT COUNT(*) FROM notes WHERE parent_id = ?1",
                [&note_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0)
            > 0;

        // Determine the new extension
        let new_extension = if has_children {
            // This is a directory - extension change means extension change but still a directory
            if let Some(new_ext) = new_ext {
                Self::normalize_extension(new_ext)
            } else {
                &current_syntax
            }
        } else {
            // This is a file - validate extension and determine extension
            if let Some(new_ext) = new_ext {
                Self::normalize_extension(new_ext)
            } else {
                "txt" // No extension defaults to txt
            }
        };

        // Get current timestamp in Australia/Sydney timezone
        let now = Self::current_timestamp();

        // Update the note with new title, parent, and extension
        let update_result = self.db.execute(
            "UPDATE notes SET title = ?1, parent_id = ?2, syntax = ?3, updated_at = ?4 WHERE id = ?5",
            rusqlite::params![new_title, new_parent_note_id, new_extension, &now, &note_id]
        );

        match update_result {
            Ok(rows_affected) => {
                if rows_affected > 0 {
                    // Successfully renamed the note
                    // Update inode mappings for the renamed item and all its descendants
                    let old_path = if parent_path == "/" {
                        format!("/{old_name}")
                    } else {
                        format!("{parent_path}/{old_name}")
                    };

                    let new_path = if new_parent_path == "/" {
                        format!("/{new_name}")
                    } else {
                        format!("{new_parent_path}/{new_name}")
                    };

                    // Collect paths to update (including descendants)
                    let mut paths_to_update = Vec::new();
                    for (path, inode) in &self.inode_map {
                        if path == &old_path || path.starts_with(&format!("{old_path}/")) {
                            let new_descendant_path = if path == &old_path {
                                new_path.clone()
                            } else {
                                path.replacen(&old_path, &new_path, 1)
                            };
                            paths_to_update.push((path.clone(), new_descendant_path, *inode));
                        }
                    }

                    // Apply the inode mapping updates
                    for (old_path, new_path, inode) in paths_to_update {
                        self.inode_map.remove(&old_path);
                        self.inode_map.insert(new_path.clone(), inode);
                        self.reverse_inode_map.insert(inode, new_path);
                    }

                    reply.ok();
                } else {
                    // No rows affected - note not found
                    reply.error(ENOENT);
                }
            }
            Err(_) => {
                // Database error
                reply.error(libc::EIO);
            }
        }
    }

    /// Handle file deletion operations (unified schema)
    ///
    /// In the unified schema, file deletion has special considerations:
    /// - Regular files: Delete the note if it has no children
    /// - Index files: Clear the content of the parent note (but don't delete the note itself)
    /// - Cannot delete notes that have children (they appear as directories)
    ///
    /// Key behaviors:
    /// - Deletes the most recent row (based on updated_at) if duplicates exist
    /// - Extracts title from filename using syntax-based extensions
    /// - Handles index.{ext} files specially by clearing parent content
    /// - Updates inode mappings to reflect the deletion
    /// - Required for proper file manager and shell integration
    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        let filename = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        // Get parent path
        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Get parent note ID (None for root level)
        let parent_note_id = if parent_path == "/" {
            None
        } else {
            match self.get_parent_folder_id(&parent_path) {
                Ok(id) => Some(id),
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // Handle special case for index files (index.{ext})
        if filename.starts_with("index.") {
            if let Some(parent_id) = &parent_note_id {
                // Get the parent note's syntax to validate the extension
                if let Ok(parent_syntax) = self.db.query_row(
                    "SELECT syntax FROM notes WHERE id = ?1",
                    [parent_id],
                    |row| row.get::<_, String>(0),
                ) {
                    // Extract the requested extension
                    if let Some(dot_pos) = filename.rfind('.') {
                        let requested_ext = &filename[dot_pos + 1..];
                        let expected_ext = &parent_syntax;

                        // Verify the extension matches the parent note's syntax
                        if requested_ext == expected_ext {
                            // Clear the content of the parent note instead of deleting it
                            let now = Self::current_timestamp();
                            let result = self.db.execute(
                                "UPDATE notes SET content = '', updated_at = ?1 WHERE id = ?2",
                                rusqlite::params!["", &now, parent_id],
                            );

                            match result {
                                Ok(rows_affected) => {
                                    if rows_affected > 0 {
                                        // Successfully cleared content
                                        // Remove from inode mappings
                                        let file_path = if parent_path == "/" {
                                            format!("/{filename}")
                                        } else {
                                            format!("{parent_path}/{filename}")
                                        };

                                        if let Some(inode) = self.inode_map.remove(&file_path) {
                                            self.reverse_inode_map.remove(&inode);
                                        }

                                        reply.ok();
                                    } else {
                                        reply.error(ENOENT);
                                    }
                                }
                                Err(_) => {
                                    reply.error(libc::EIO);
                                }
                            }
                        } else {
                            // Extension doesn't match the note's syntax
                            reply.error(ENOENT);
                        }
                    } else {
                        // No extension in filename
                        reply.error(ENOENT);
                    }
                } else {
                    // Parent note not found
                    reply.error(ENOENT);
                }
                return;
            }
        }

        // Handle special editor files (backup, swap, temporary files)
        if Self::is_editor_temp_file(filename) {
            // For editor temporary files, just reply OK without doing anything
            // This allows editors like Neovim to create and delete backup files
            reply.ok();
            return;
        }

        // Handle regular file deletion
        // Extract title and extension from filename
        let (title, requested_ext) = if let Some(dot_pos) = filename.rfind('.') {
            (&filename[..dot_pos], Some(&filename[dot_pos + 1..]))
        } else {
            (filename, None)
        };

        // Find the note to delete and validate extension matches syntax
        let note_query = "SELECT id, syntax FROM notes WHERE parent_id IS ?1 AND title = ?2 ORDER BY updated_at DESC LIMIT 1";

        let note_result = self.db.query_row(
            note_query,
            rusqlite::params![parent_note_id, title],
            |row| {
                let id: String = row.get(0)?;
                let syntax: String = row.get(1)?;
                Ok((id, syntax))
            },
        );

        let (note_id, note_syntax) = match note_result {
            Ok(result) => result,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        // Validate that the requested extension matches the note's syntax
        if let Some(ext) = requested_ext {
            let expected_ext = &note_syntax;
            if ext != expected_ext {
                // Extension doesn't match the note's syntax
                reply.error(ENOENT);
                return;
            }
        }

        // Check if this note has children (if so, it's a directory and cannot be deleted as a file)
        let has_children = self
            .db
            .query_row(
                "SELECT COUNT(*) FROM notes WHERE parent_id = ?1",
                [&note_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0)
            > 0;

        if has_children {
            // This note has children, so it's a directory - cannot delete as a file
            reply.error(libc::EISDIR);
            return;
        }

        // Delete the note (it's a leaf note with no children)
        let result = self
            .db
            .execute("DELETE FROM notes WHERE id = ?1", [&note_id]);

        match result {
            Ok(rows_affected) => {
                if rows_affected > 0 {
                    // Successfully deleted the file
                    // Remove from inode mappings
                    let file_path = if parent_path == "/" {
                        format!("/{filename}")
                    } else {
                        format!("{parent_path}/{filename}")
                    };

                    if let Some(inode) = self.inode_map.remove(&file_path) {
                        self.reverse_inode_map.remove(&inode);
                    }

                    reply.ok();
                } else {
                    // Should not happen since we just queried for it
                    reply.error(ENOENT);
                }
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }

    /// Handle directory deletion operations (unified schema)
    ///
    /// In the unified schema, deleting a directory means deleting a note that has children.
    /// The note acts as a folder, and we need to ensure it's empty before deletion.
    ///
    /// Key behaviors:
    /// - Only deletes empty directories (standard rmdir behavior)
    /// - Deletes the most recent note (based on updated_at) if duplicates exist
    /// - Updates inode mappings to reflect the deletion
    /// - Required for proper file manager and shell integration
    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        let dirname = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        // Get parent path
        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Get parent note ID from database (unified schema)
        let parent_note_id = if parent_path == "/" {
            None
        } else {
            match self.get_parent_folder_id(&parent_path) {
                Ok(id) => Some(id),
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // First, get the note ID that represents the directory we want to delete
        let note_to_delete_id: Result<String, rusqlite::Error> = self.db.query_row(
            "SELECT id FROM notes
             WHERE parent_id IS ?1 AND title = ?2
             ORDER BY updated_at DESC
             LIMIT 1",
            rusqlite::params![parent_note_id, dirname],
            |row| row.get(0),
        );

        let note_id = match note_to_delete_id {
            Ok(id) => id,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        // Check if the directory is empty (no child notes)
        let child_count: Result<i64, rusqlite::Error> = self.db.query_row(
            "SELECT COUNT(*) FROM notes WHERE parent_id = ?1",
            [&note_id],
            |row| row.get(0),
        );

        match child_count {
            Ok(count) => {
                if count > 0 {
                    // Directory is not empty
                    reply.error(libc::ENOTEMPTY);
                    return;
                }
            }
            Err(_) => {
                reply.error(libc::EIO);
                return;
            }
        }

        // Directory is empty, proceed with deletion
        let result = self
            .db
            .execute("DELETE FROM notes WHERE id = ?1", [&note_id]);

        match result {
            Ok(rows_affected) => {
                if rows_affected > 0 {
                    // Successfully deleted the directory
                    // Remove from inode mappings
                    let dir_path = if parent_path == "/" {
                        format!("/{dirname}")
                    } else {
                        format!("{parent_path}/{dirname}")
                    };

                    if let Some(inode) = self.inode_map.remove(&dir_path) {
                        self.reverse_inode_map.remove(&inode);
                    }

                    // Also remove any index file mappings for this directory
                    for ext in Self::SUPPORTED_EXTENSIONS {
                        let index_path = format!("{}/index.{}", dir_path, ext);
                        if let Some(index_inode) = self.inode_map.remove(&index_path) {
                            self.reverse_inode_map.remove(&index_inode);
                        }
                    }

                    reply.ok();
                } else {
                    // Should not happen since we just queried for it
                    reply.error(ENOENT);
                }
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }
}
