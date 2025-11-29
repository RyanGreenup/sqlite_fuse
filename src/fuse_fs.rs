use std::{
    collections::{HashMap, HashSet},
    error::Error,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const DEBUG: bool = true;


use chrono::{DateTime, Utc};
use chrono_tz::{Australia::Sydney, Tz};
const TIMEZONE: Tz = Sydney; // Australia/Sydney timezone

use fuser::Filesystem;

use libc::ENOENT;
use rusqlite::Connection;
use std::ffi::OsStr;

use fuser::{FileAttr, FileType, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request};

use crate::database::Database;

const TTL: Duration = Duration::from_secs(1); // 1 second

pub struct ExampleFuseFs {
    inode_map: HashMap<String, u64>,
    reverse_inode_map: HashMap<u64, String>,
    next_inode: u64,
    db: Database,
    user_id: String,
}

impl ExampleFuseFs {
    fn is_dir(&self, path: &str) -> bool {
        if path == "/" {
            return true; // Root is always a directory
        }
        let db_path = Self::normalize_path_for_db(path);
        match self.db.get_folder_id_by_path(db_path, self.user_id.as_str()) {
            Ok(Some(_id)) => true,  // Folder exists
            Ok(None) => false,       // Not a folder (file or doesn't exist)
            Err(_e) => false,        // Database error
        }
    }

    /// Converts a chrono::DateTime<Utc> to SystemTime for FUSE file attributes
    fn datetime_to_systemtime(dt: &DateTime<Utc>) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(dt.timestamp() as u64)
    }

    fn split_parent_path_and_filename(path: &str) -> (String, String) {
        let (parent_path, filename) = if let Some(pos) = path.rfind('/') {
            let parent = &path[..pos];
            let name = &path[pos + 1..];
            (if parent.is_empty() { "/" } else { parent }, name)
        } else {
            ("/", path)
        };
        (parent_path.to_string(), filename.to_string())
    }

    /// Normalize a FUSE path for database queries
    ///
    /// The FUSE layer uses paths with leading slashes (e.g., "/1", "/1/2"),
    /// but the database stores paths without leading slashes (e.g., "1", "1/2").
    /// This function strips the leading slash for database queries.
    ///
    /// Special case: "/" (root) remains as "/" since it has special handling
    fn normalize_path_for_db(fuse_path: &str) -> &str {
        if fuse_path == "/" {
            "/"
        } else if let Some(stripped) = fuse_path.strip_prefix('/') {
            stripped
        } else {
            fuse_path
        }
    }

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

    fn is_system_file(path: &str) -> bool {
        // Extract filename from path
        let filename = path.split('/').next_back().unwrap_or(path);

        // Common system files that programs try to access

        // Shared libraries
        filename.ends_with(".so") ||
        filename.ends_with(".so.1") ||
        filename.ends_with(".so.6") ||
        filename == "glibc-hwcaps" ||
        filename.starts_with("lib") && filename.contains(".so") ||

        // Filesystem metadata files
        filename == ".Trash" ||
        filename.starts_with(".Trash-") ||
        filename == "BDMV" ||
        filename == ".xdg-volume-info" ||
        filename == "autorun.inf" ||
        filename == ".hidden" ||
        filename == "System Volume Information" ||
        filename == "$RECYCLE.BIN"
    }

    fn is_editor_temp_file(filename: &str) -> bool {
        // ignore all dotfiles
        if filename.starts_with('.') {
            return true;
        }
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

    fn current_timestamp() -> String {
        let utc_now = Utc::now();
        let sydney_time = utc_now.with_timezone(&TIMEZONE);
        sydney_time.format("%Y-%m-%d %H:%M:%S").to_string()
    }

    pub fn new(connection: Connection, timezone: Option<Tz>, user_id: String) -> Result<Self, Box<dyn Error>> {
        /*
        // Create performance indexes for unified notes table
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_notes_parent_title ON notes(parent_id, title)",
            [],
        )?;
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_notes_parent_updated ON notes(parent_id, updated_at DESC)",
            [],
        )?;
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_notes_parent_id ON notes(parent_id)",
            [],
        )?;
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_notes_user_id ON notes(user_id)",
            [],
        )?;
        */
        let db = Database::new(connection, timezone);

        let mut fs = ExampleFuseFs {
            db,
            inode_map: HashMap::new(),
            reverse_inode_map: HashMap::new(),
            next_inode: 2,
            user_id,
        };

        // Root directory gets inode 1
        fs.inode_map.insert("/".to_string(), 1);
        fs.reverse_inode_map.insert(1, "/".to_string());

        Ok(fs)
    }
    fn update_inode_mappings(&mut self, old_path: &str, new_path: &str) {
        // Collect paths to update (including descendants)
        let mut paths_to_update = Vec::new();
        for (path, inode) in &self.inode_map {
            if path == old_path || path.starts_with(&format!("{old_path}/")) {
                let new_descendant_path = if path == old_path {
                    new_path.to_string()
                } else {
                    path.replacen(old_path, new_path, 1)
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

        eprintln!("[DEBUG] lookup: parent={parent}, name={name_str}");

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

        // Normalize path for database queries
        let db_path = Self::normalize_path_for_db(&full_path);

        // First, check if it's a folder/directory
        match self.db.get_folder_id_by_path(db_path, self.user_id.as_str()) {
            Ok(Some(folder_id)) => {
                // It's a directory - retrieve full folder object for timestamps
                match self.db.get_folder_by_id(&folder_id, self.user_id.as_str()) {
                    Ok(Some(folder)) => {
                        let inode = self.get_or_create_inode(&full_path);
                        let attr = FileAttr {
                            ino: inode,
                            size: 0,
                            blocks: 0,
                            atime: Self::datetime_to_systemtime(&folder.updated_at),
                            mtime: Self::datetime_to_systemtime(&folder.updated_at),
                            ctime: Self::datetime_to_systemtime(&folder.updated_at),
                            crtime: Self::datetime_to_systemtime(&folder.created_at),
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
                    }
                    Ok(None) => {
                        // Folder ID found but folder doesn't exist - database inconsistency
                        eprintln!("[ERROR] lookup: Folder ID found but folder object not retrieved: {folder_id}");
                        reply.error(ENOENT);
                        return;
                    }
                    Err(e) => {
                        eprintln!("[ERROR] lookup: Failed to get folder by ID {folder_id}: {e}");
                        reply.error(ENOENT);
                        return;
                    }
                }
            }
            Ok(None) => {
                // Not a directory, continue to check if it's a note
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] lookup: Database error checking for folder {full_path}: {e}"
                );
                reply.error(ENOENT);
                return;
            }
        }

        // Second, check if it's a note/file
        match self.db.get_note_id_by_path(db_path) {
            Ok(Some(note_id)) => {
                // It's a note/file
                match self.db.get_note_by_id(&note_id) {
                    Ok(Some(note)) => {
                        let inode = self.get_or_create_inode(&full_path);
                        let content_size = note.content.len();

                        let attr = FileAttr {
                            ino: inode,
                            size: content_size as u64,
                            blocks: content_size.div_ceil(512) as u64,
                            atime: Self::datetime_to_systemtime(&note.updated_at),
                            mtime: Self::datetime_to_systemtime(&note.updated_at),
                            ctime: Self::datetime_to_systemtime(&note.updated_at),
                            crtime: Self::datetime_to_systemtime(&note.created_at),
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
                    }
                    Ok(None) => {
                        eprintln!(
                            "[ERROR] lookup: Note with id {note_id} not found in database"
                        );
                        reply.error(ENOENT);
                    }
                    Err(e) => {
                        eprintln!(
                            "[ERROR] lookup: Database error retrieving note {note_id}: {e}"
                        );
                        reply.error(ENOENT);
                    }
                }
            }
            Ok(None) => {
                // Neither a directory nor a note - doesn't exist
                eprintln!("[DEBUG] lookup: Path {full_path} not found in database");
                reply.error(ENOENT);
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] lookup: Database error checking for note {full_path}: {e}"
                );
                reply.error(ENOENT);
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        eprintln!("[DEBUG] getattr: ino={ino}");

        // Handle root directory specially
        if ino == 1 {
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

        // Get path from inode
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Normalize path for database queries
        let db_path = Self::normalize_path_for_db(&path);

        // First, check if it's a folder/directory
        match self.db.get_folder_id_by_path(db_path, self.user_id.as_str()) {
            Ok(Some(folder_id)) => {
                // It's a directory - retrieve full folder object for timestamps
                match self.db.get_folder_by_id(&folder_id, self.user_id.as_str()) {
                    Ok(Some(folder)) => {
                        let attr = FileAttr {
                            ino,
                            size: 0,
                            blocks: 0,
                            atime: Self::datetime_to_systemtime(&folder.updated_at),
                            mtime: Self::datetime_to_systemtime(&folder.updated_at),
                            ctime: Self::datetime_to_systemtime(&folder.updated_at),
                            crtime: Self::datetime_to_systemtime(&folder.created_at),
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
                    Ok(None) => {
                        eprintln!("[ERROR] getattr: Folder ID found but folder object not retrieved: {folder_id}");
                        reply.error(ENOENT);
                        return;
                    }
                    Err(e) => {
                        eprintln!("[ERROR] getattr: Failed to get folder by ID {folder_id}: {e}");
                        reply.error(ENOENT);
                        return;
                    }
                }
            }
            Ok(None) => {
                // Not a directory, continue to check if it's a note
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] getattr: Database error checking for folder {path}: {e}"
                );
                reply.error(ENOENT);
                return;
            }
        }

        // Second, check if it's a note/file
        match self.db.get_note_id_by_path(db_path) {
            Ok(Some(note_id)) => {
                // It's a note/file, get the note content
                match self.db.get_note_by_id(&note_id) {
                    Ok(Some(note)) => {
                        let size = note.content.len() as u64;
                        let blocks = note.content.len().div_ceil(512) as u64;

                        let attr = FileAttr {
                            ino,
                            size,
                            blocks,
                            atime: Self::datetime_to_systemtime(&note.updated_at),
                            mtime: Self::datetime_to_systemtime(&note.updated_at),
                            ctime: Self::datetime_to_systemtime(&note.updated_at),
                            crtime: Self::datetime_to_systemtime(&note.created_at),
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
                    }
                    Ok(None) => {
                        eprintln!(
                            "[ERROR] getattr: Note with id {note_id} not found in database"
                        );
                        reply.error(ENOENT);
                    }
                    Err(e) => {
                        eprintln!(
                            "[ERROR] getattr: Database error retrieving note {note_id}: {e}"
                        );
                        reply.error(ENOENT);
                    }
                }
            }
            Ok(None) => {
                // Neither a directory nor a note - doesn't exist
                eprintln!("[DEBUG] getattr: Path {path} not found in database");
                reply.error(ENOENT);
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] getattr: Database error checking for note {path}: {e}"
                );
                reply.error(ENOENT);
            }
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
        eprintln!("[DEBUG] read: ino={ino}, offset={offset}");

        // Get path from inode
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Normalize path for database queries
        let db_path = Self::normalize_path_for_db(&path);

        // Check if it's a directory - directories cannot be read as files
        match self.db.get_folder_id_by_path(db_path, self.user_id.as_str()) {
            Ok(Some(_folder_id)) => {
                // It's a directory - cannot read as file
                eprintln!("[ERROR] read: Attempted to read directory {path} as file");
                reply.error(libc::EISDIR);
                return;
            }
            Ok(None) => {
                // Not a directory, continue to check if it's a note
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] read: Database error checking for folder {path}: {e}"
                );
                reply.error(ENOENT);
                return;
            }
        }

        // Check if it's a note/file
        match self.db.get_note_id_by_path(db_path) {
            Ok(Some(note_id)) => {
                // It's a note/file, get the content
                match self.db.get_note_by_id(&note_id) {
                    Ok(Some(note)) => {
                        let content_bytes = note.content.as_bytes();
                        let start = offset as usize;

                        if start < content_bytes.len() {
                            reply.data(&content_bytes[start..]);
                        } else {
                            // Offset beyond file content, return empty data
                            reply.data(&[]);
                        }
                    }
                    Ok(None) => {
                        eprintln!(
                            "[ERROR] read: Note with id {note_id} not found in database"
                        );
                        reply.error(ENOENT);
                    }
                    Err(e) => {
                        eprintln!(
                            "[ERROR] read: Database error retrieving note {note_id}: {e}"
                        );
                        reply.error(ENOENT);
                    }
                }
            }
            Ok(None) => {
                // Neither a directory nor a note - doesn't exist
                eprintln!("[DEBUG] read: Path {path} not found in database");
                reply.error(ENOENT);
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] read: Database error checking for note {path}: {e}"
                );
                reply.error(ENOENT);
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        eprintln!("[DEBUG] readdir: ino={ino}, offset={offset}");

        // Get path from inode
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Check if it's actually a directory before reading it
        let folder_id = if path == "/" {
            // Root directory - special case
            None
        } else {
            let db_path = Self::normalize_path_for_db(&path);
            match self.db.get_folder_id_by_path(db_path, self.user_id.as_str()) {
                Ok(Some(id)) => Some(id),
                Ok(None) => {
                    // Not a directory - cannot readdir on a file
                    eprintln!(
                        "[ERROR] readdir: Attempted to readdir on non-directory {path}"
                    );
                    reply.error(libc::ENOTDIR);
                    return;
                }
                Err(e) => {
                    eprintln!(
                        "[ERROR] readdir: Database error checking for folder {path}: {e}"
                    );
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // Determine parent inode for ".." entry
        let parent_ino = if path == "/" {
            1 // Root's parent is itself
        } else if let Some(pos) = path.rfind('/') {
            let parent_path = if pos == 0 { "/" } else { &path[..pos] };
            self.inode_map.get(parent_path).copied().unwrap_or(1)
        } else {
            1
        };

        // Start with standard directory entries
        let mut entries = vec![
            (ino, FileType::Directory, ".".to_string()),
            (parent_ino, FileType::Directory, "..".to_string()),
        ];

        // Get directory contents from database
        let mut seen_names: HashSet<String> = HashSet::new();

        // Handle root vs non-root directories
        if path == "/" {
            // Root directory - get top-level folders and notes

            // Get root folders
            match self.db.list_folders_by_parent(None, self.user_id.as_str()) {
                Ok(folders) => {
                    for folder in folders {
                        let name = folder.title.clone();
                        if !seen_names.contains(&name) {
                            seen_names.insert(name.clone());
                            let folder_path = format!("/{name}");
                            let child_ino = self.get_or_create_inode(&folder_path);
                            entries.push((child_ino, FileType::Directory, name));
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[ERROR] readdir: Unable to get root folders: {e}");
                    reply.error(ENOENT);
                    return;
                }
            }

            // Get root notes
            match self.db.list_notes_by_parent(None, self.user_id.as_str()) {
                Ok(notes) => {
                    for note in notes {
                        let filename = format!("{}.{}", note.title, note.syntax);
                        if !seen_names.contains(&filename) {
                            seen_names.insert(filename.clone());
                            let file_path = format!("/{filename}");
                            let child_ino = self.get_or_create_inode(&file_path);
                            entries.push((child_ino, FileType::RegularFile, filename));
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[ERROR] readdir: Unable to get root notes: {e}");
                    reply.error(ENOENT);
                    return;
                }
            }
        } else {
            // Non-root directory - get direct children only
            let current_folder_id = folder_id.unwrap();

            // Get direct child folders
            match self.db.list_folders_by_parent(Some(&current_folder_id), self.user_id.as_str()) {
                Ok(folders) => {
                    for folder in folders {
                        let name = folder.title.clone();
                        if !seen_names.contains(&name) {
                            seen_names.insert(name.clone());
                            let folder_path = if path == "/" {
                                format!("/{name}")
                            } else {
                                format!("{path}/{name}")
                            };
                            let child_ino = self.get_or_create_inode(&folder_path);
                            entries.push((child_ino, FileType::Directory, name));
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[ERROR] readdir: Unable to get child folders for {path}: {e}"
                    );
                    reply.error(ENOENT);
                    return;
                }
            }

            // Get direct child notes
            match self
                .db
                .list_notes_by_parent(Some(&current_folder_id), self.user_id.as_str())
            {
                Ok(notes) => {
                    for note in notes {
                        let filename = format!("{}.{}", note.title, note.syntax);
                        if !seen_names.contains(&filename) {
                            seen_names.insert(filename.clone());
                            let file_path = if path == "/" {
                                format!("/{filename}")
                            } else {
                                format!("{path}/{filename}")
                            };
                            let child_ino = self.get_or_create_inode(&file_path);
                            entries.push((child_ino, FileType::RegularFile, filename));
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[ERROR] readdir: Unable to get child notes for {path}: {e}"
                    );
                    reply.error(ENOENT);
                    return;
                }
            }
        }

        // Return entries starting from the requested offset
        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            // i + 1 is the offset for the next entry
            if reply.add(entry.0, (i + 1) as i64, entry.1, entry.2) {
                break; // Buffer full
            }
        }

        reply.ok();
    }

    /// Handle directory creation operations
    ///
    /// Key behaviors:
    /// - Creates a folder in the database
    /// - Validates parent directory exists
    /// - Checks for name conflicts
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

        eprintln!("[DEBUG] mkdir: parent={parent}, name={folder_name}");

        // Get parent path
        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Create the full path for the new directory
        let full_path = if parent_path == "/" {
            format!("/{folder_name}")
        } else {
            format!("{parent_path}/{folder_name}")
        };

        // Normalize paths for database queries
        let db_path = Self::normalize_path_for_db(&full_path);

        // Check if directory already exists
        match self.db.get_folder_id_by_path(db_path, self.user_id.as_str()) {
            Ok(Some(_existing_id)) => {
                eprintln!("[ERROR] mkdir: Directory {full_path} already exists");
                reply.error(libc::EEXIST);
                return;
            }
            Ok(None) => {
                // Good, directory doesn't exist
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] mkdir: Database error checking for existing directory {full_path}: {e}"
                );
                reply.error(libc::EIO);
                return;
            }
        }

        // Check if a file/note with the same name exists
        match self.db.get_note_id_by_path(db_path) {
            Ok(Some(_existing_id)) => {
                eprintln!("[ERROR] mkdir: File {full_path} already exists");
                reply.error(libc::EEXIST);
                return;
            }
            Ok(None) => {
                // Good, no file with this name
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] mkdir: Database error checking for existing file {full_path}: {e}"
                );
                reply.error(libc::EIO);
                return;
            }
        }

        // Get parent folder ID - None for root, Some(id) for other paths
        let parent_id = if parent_path == "/" {
            None
        } else {
            let db_parent_path = Self::normalize_path_for_db(&parent_path);
            match self.db.get_folder_id_by_path(db_parent_path, self.user_id.as_str()) {
                Ok(Some(id)) => Some(id),
                Ok(None) => {
                    eprintln!("[ERROR] mkdir: Parent directory {parent_path} not found");
                    reply.error(ENOENT);
                    return;
                }
                Err(e) => {
                    eprintln!(
                        "[ERROR] mkdir: Database error checking parent directory {parent_path}: {e}"
                    );
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // Create the folder in the database
        let _folder_id = match self.db.create_folder(folder_name, parent_id.as_deref(), self.user_id.as_str()) {
            Ok(id) => id,
            Err(e) => {
                eprintln!(
                    "[ERROR] mkdir: Unable to create folder {full_path}: {e}"
                );
                reply.error(libc::EIO);
                return;
            }
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

    /// Handle file creation operations
    ///
    /// Creates a new note in the database. The file extension determines the syntax type.
    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let file_name = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        eprintln!(
            "[DEBUG] create: parent={parent}, name={file_name}, mode={mode:#o}, flags={flags:#x}"
        );

        // Get parent path
        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Construct the full path
        let full_path = if parent_path == "/" {
            format!("/{file_name}")
        } else {
            format!("{parent_path}/{file_name}")
        };

        // Normalize path for database queries
        let db_path = Self::normalize_path_for_db(&full_path);

        // Check if file already exists
        match self.db.get_note_id_by_path(db_path) {
            Ok(Some(_existing_id)) => {
                eprintln!("[ERROR] create: File {full_path} already exists");
                reply.error(libc::EEXIST);
                return;
            }
            Ok(None) => {
                // File doesn't exist, good to proceed
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] create: Database error checking for existing file {full_path}: {e}"
                );
                reply.error(libc::EIO);
                return;
            }
        }

        // Handle editor temporary files by creating them as regular empty files
        // but don't store them in the database
        if Self::is_editor_temp_file(file_name) {
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

        // Parse file name to extract title and syntax
        let file_name_path = Path::new(file_name);
        let title = match file_name_path.file_stem() {
            Some(stem) => stem.to_string_lossy().into_owned(),
            None => {
                eprintln!(
                    "[ERROR] create: Unable to extract title from filename {file_name}"
                );
                reply.error(libc::EINVAL);
                return;
            }
        };

        let syntax = match file_name_path.extension() {
            Some(ext) => ext.to_string_lossy().into_owned(),
            None => {
                eprintln!(
                    "[ERROR] create: All files must have an extension (e.g., {file_name}.txt, {file_name}.md)"
                );
                reply.error(libc::EINVAL);
                return;
            }
        };

        // Get parent folder ID - None for root, Some(id) for other paths
        let parent_folder_id = if parent_path == "/" {
            None
        } else {
            let db_parent_path = Self::normalize_path_for_db(&parent_path);
            match self.db.get_folder_id_by_path(db_parent_path, self.user_id.as_str()) {
                Ok(Some(id)) => Some(id),
                Ok(None) => {
                    eprintln!("[ERROR] create: Parent directory {parent_path} not found");
                    reply.error(ENOENT);
                    return;
                }
                Err(e) => {
                    eprintln!(
                        "[ERROR] create: Database error checking parent directory {parent_path}: {e}"
                    );
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // Create new note in database
        let note_id = format!("{:x}", uuid::Uuid::new_v4().as_simple());
        let content = "";
        let abstract_text = Some("");

        match self.db.create_note(
            &note_id,
            &title,
            abstract_text,
            content,
            &syntax,
            parent_folder_id.as_deref(),
            self.user_id.as_str(),
        ) {
            Ok(_created_id) => {
                // Note created successfully
                let inode = self.get_or_create_inode(&full_path);
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

                reply.created(&TTL, &attr, 0, inode, 0);
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] create: Failed to create note in database for {full_path}: {e}"
                );
                reply.error(libc::EIO);
            }
        }
    }

    /// Handle file write operations
    ///
    /// This method handles writing to regular files
    /// The content is immediately written to the database's 'content' field.
    ///
    /// Key behaviors:
    /// - offset 0: Completely overwrites existing content
    /// - offset > 0: Inserts/appends data at the specified position
    /// - Updates timestamps (updated_at) in database
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
        eprintln!(
            "[DEBUG] write: ino={}, offset={}, data_len={}",
            ino,
            offset,
            data.len()
        );

        // Get path from inode
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Normalize path for database queries
        let db_path = Self::normalize_path_for_db(&path);

        // Check if it's a directory - can't write to directories
        match self.db.get_folder_id_by_path(db_path, self.user_id.as_str()) {
            Ok(Some(_folder_id)) => {
                reply.error(libc::EISDIR);
                return;
            }
            Ok(None) => {
                // Not a directory, continue to check if it's a note
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] write: Database error checking for folder {path}: {e}"
                );
                reply.error(ENOENT);
                return;
            }
        }

        // Get the note ID and current content
        let (note_id, current_content) = match self.db.get_note_id_by_path(db_path) {
            Ok(Some(note_id)) => {
                // Get the note content
                match self.db.get_note_by_id(&note_id) {
                    Ok(Some(note)) => (note_id, note.content),
                    Ok(None) => {
                        eprintln!(
                            "[ERROR] write: Note with id {note_id} not found in database"
                        );
                        reply.error(ENOENT);
                        return;
                    }
                    Err(e) => {
                        eprintln!(
                            "[ERROR] write: Database error retrieving note {note_id}: {e}"
                        );
                        reply.error(ENOENT);
                        return;
                    }
                }
            }
            Ok(None) => {
                eprintln!("[DEBUG] write: File {path} not found in database");
                reply.error(ENOENT);
                return;
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] write: Database error checking for note {path}: {e}"
                );
                reply.error(ENOENT);
                return;
            }
        };

        // Calculate new content based on offset and data
        let new_content = if offset == 0 {
            // Overwrite from the beginning
            String::from_utf8_lossy(data).to_string()
        } else {
            // Insert/append at offset
            let mut content_bytes = current_content.into_bytes();
            let start_pos = offset as usize;

            if start_pos > content_bytes.len() {
                // If offset is beyond current content, pad with zeros
                content_bytes.resize(start_pos, 0);
            }

            // Replace or extend content
            if start_pos + data.len() <= content_bytes.len() {
                // Replace existing content at offset
                content_bytes[start_pos..start_pos + data.len()].copy_from_slice(data);
            } else {
                // Extend content - truncate at offset and append new data
                content_bytes.truncate(start_pos);
                content_bytes.extend_from_slice(data);
            }

            String::from_utf8_lossy(&content_bytes).to_string()
        };

        // Update the note content in the database
        // First get the note again to preserve title, syntax, etc.
        let note = match self.db.get_note_by_id(&note_id) {
            Ok(Some(note)) => note,
            Ok(None) => {
                eprintln!("[ERROR] write: Note disappeared during write operation");
                reply.error(ENOENT);
                return;
            }
            Err(e) => {
                eprintln!("[ERROR] write: Database error re-retrieving note: {e}");
                reply.error(libc::EIO);
                return;
            }
        };

        // Update the note with new content
        match self.db.update_note(
            &note_id,
            &note.title,
            note.abstract_text.as_deref(),
            &new_content,
            &note.syntax,
        ) {
            Ok(_success) => {
                reply.written(data.len() as u32);
            }
            Err(e) => {
                eprintln!("[ERROR] write: Failed to update note content: {e}");
                reply.error(libc::EIO);
            }
        }
    }

    /// Handle file opening operations
    ///
    /// This method verifies that a file exists before allowing it to be opened.
    fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        eprintln!("[DEBUG] open: ino={ino}, flags={flags:#x}");

        // Get path from inode
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Normalize path for database queries
        let db_path = Self::normalize_path_for_db(&path);

        // First, check if it's a folder/directory - can't open directories as files
        match self.db.get_folder_id_by_path(db_path, self.user_id.as_str()) {
            Ok(Some(_folder_id)) => {
                // It's a directory - return error since we're trying to open it as a file
                reply.error(libc::EISDIR);
                return;
            }
            Ok(None) => {
                // Not a directory, continue to check if it's a note/file
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] open: Database error checking for folder {path}: {e}"
                );
                reply.error(ENOENT);
                return;
            }
        }

        // Second, check if it's a note/file
        match self.db.get_note_id_by_path(db_path) {
            Ok(Some(_note_id)) => {
                // It's a valid file - allow opening
                reply.opened(ino, 0);
            }
            Ok(None) => {
                // Neither a directory nor a note - doesn't exist
                eprintln!("[DEBUG] open: File {path} not found in database");
                reply.error(ENOENT);
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] open: Database error checking for note {path}: {e}"
                );
                reply.error(ENOENT);
            }
        }
    }

    /// Handle file attribute setting operations
    ///
    /// Key behaviors:
    /// - Handles size changes (truncation/extension of file content)
    /// - Updates timestamps in the database when modified
    /// - Validates that the file exists before making changes
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
        eprintln!("[DEBUG] setattr: ino={ino}, size={size:?}");

        // Get path from inode
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Normalize path for database queries
        let db_path = Self::normalize_path_for_db(&path);

        // First, check if it's a folder/directory
        match self.db.get_folder_id_by_path(db_path, self.user_id.as_str()) {
            Ok(Some(folder_id)) => {
                // It's a directory - retrieve full folder object for timestamps
                match self.db.get_folder_by_id(&folder_id, self.user_id.as_str()) {
                    Ok(Some(folder)) => {
                        let attr = FileAttr {
                            ino,
                            size: 0,
                            blocks: 0,
                            atime: Self::datetime_to_systemtime(&folder.updated_at),
                            mtime: Self::datetime_to_systemtime(&folder.updated_at),
                            ctime: Self::datetime_to_systemtime(&folder.updated_at),
                            crtime: Self::datetime_to_systemtime(&folder.created_at),
                            kind: FileType::Directory,
                            perm: mode.unwrap_or(0o755) as u16,
                            nlink: 2,
                            uid: uid.unwrap_or(501),
                            gid: gid.unwrap_or(20),
                            rdev: 0,
                            flags: 0,
                            blksize: 512,
                        };
                        reply.attr(&TTL, &attr);
                        return;
                    }
                    Ok(None) => {
                        eprintln!("[ERROR] setattr: Folder ID found but folder object not retrieved: {folder_id}");
                        reply.error(ENOENT);
                        return;
                    }
                    Err(e) => {
                        eprintln!("[ERROR] setattr: Failed to get folder by ID {folder_id}: {e}");
                        reply.error(ENOENT);
                        return;
                    }
                }
            }
            Ok(None) => {
                // Not a directory, continue to check if it's a note/file
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] setattr: Database error checking for folder {path}: {e}"
                );
                reply.error(ENOENT);
                return;
            }
        }

        // Second, check if it's a note/file and get current content
        let (note_id, mut note) = match self.db.get_note_id_by_path(db_path) {
            Ok(Some(note_id)) => {
                // Get the note content
                match self.db.get_note_by_id(&note_id) {
                    Ok(Some(note)) => (note_id, note),
                    Ok(None) => {
                        eprintln!(
                            "[ERROR] setattr: Note with id {note_id} not found in database"
                        );
                        reply.error(ENOENT);
                        return;
                    }
                    Err(e) => {
                        eprintln!(
                            "[ERROR] setattr: Database error retrieving note {note_id}: {e}"
                        );
                        reply.error(ENOENT);
                        return;
                    }
                }
            }
            Ok(None) => {
                // Neither a directory nor a note - doesn't exist
                eprintln!("[DEBUG] setattr: File {path} not found in database");
                reply.error(ENOENT);
                return;
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] setattr: Database error checking for note {path}: {e}"
                );
                reply.error(ENOENT);
                return;
            }
        };

        // Handle size changes (file truncation/extension)
        if let Some(new_size) = size {
            let mut content_bytes = note.content.clone().into_bytes();
            let target_size = new_size as usize;

            // Adjust content size based on target
            if target_size < content_bytes.len() {
                content_bytes.truncate(target_size);
            } else if target_size > content_bytes.len() {
                content_bytes.resize(target_size, 0);
            }

            // Update content in database
            let new_content = String::from_utf8_lossy(&content_bytes).to_string();
            match self.db.update_note(
                &note_id,
                &note.title,
                note.abstract_text.as_deref(),
                &new_content,
                &note.syntax,
            ) {
                Ok(_success) => {
                    // Re-fetch the note to get updated timestamps from database
                    match self.db.get_note_by_id(&note_id) {
                        Ok(Some(updated_note)) => {
                            note = updated_note;
                        }
                        Ok(None) | Err(_) => {
                            // If we can't re-fetch, just update the content locally
                            note.content = new_content;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[ERROR] setattr: Failed to update note content: {e}");
                    reply.error(libc::EIO);
                    return;
                }
            };
        }

        // Calculate file size and blocks
        let file_size = note.content.len() as u64;
        let file_blocks = note.content.len().div_ceil(512) as u64;

        // Return updated file attributes
        let attr = FileAttr {
            ino,
            size: file_size,
            blocks: file_blocks,
            atime: Self::datetime_to_systemtime(&note.updated_at),
            mtime: Self::datetime_to_systemtime(&note.updated_at),
            ctime: Self::datetime_to_systemtime(&note.updated_at),
            crtime: Self::datetime_to_systemtime(&note.created_at),
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

    /// Handle file and directory renaming operations
    ///
    /// Key behaviors:
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

        eprintln!("[DEBUG] rename: {old_name} -> {new_name}");

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

        // Construct old and new paths
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

        // Normalize paths for database queries
        let db_old_path = Self::normalize_path_for_db(&old_path);

        // Get the new parent ID for database operations
        let new_parent_id = if new_parent_path == "/" {
            None
        } else {
            let db_new_parent_path = Self::normalize_path_for_db(&new_parent_path);
            match self.db.get_folder_id_by_path(db_new_parent_path, self.user_id.as_str()) {
                Ok(maybe_id) => maybe_id,
                Err(e) => {
                    eprintln!(
                        "[ERROR] rename: Database error checking for new parent folder {new_parent_path}: {e}"
                    );
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // First, check if it's a directory being renamed
        match self.db.get_folder_id_by_path(db_old_path, self.user_id.as_str()) {
            Ok(Some(folder_id)) => {
                // It's a directory - update both name and parent
                match self.db.update_folder(&folder_id, new_name, self.user_id.as_str()) {
                    Ok(_success) => {
                        // Also update the parent relationship
                        match self
                            .db
                            .update_folder_parent(&folder_id, new_parent_id.as_deref(), self.user_id.as_str())
                        {
                            Ok(_success) => {
                                self.update_inode_mappings(&old_path, &new_path);
                                reply.ok();
                                return;
                            }
                            Err(e) => {
                                eprintln!("[ERROR] rename: Failed to update folder parent: {e}");
                                reply.error(libc::EIO);
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[ERROR] rename: Failed to update folder: {e}");
                        reply.error(libc::EIO);
                        return;
                    }
                }
            }
            Ok(None) => {
                // Not a directory, continue to check if it's a note
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] rename: Database error checking for folder {old_path}: {e}"
                );
                reply.error(ENOENT);
                return;
            }
        }

        // Second, check if it's a note/file being renamed
        match self.db.get_note_id_by_path(db_old_path) {
            Ok(Some(note_id)) => {
                // It's a note/file - get the note and update it
                match self.db.get_note_by_id(&note_id) {
                    Ok(Some(note)) => {
                        // Extract title and extension from new filename
                        let file_name_path = Path::new(new_name);
                        let title = match file_name_path.file_stem() {
                            Some(stem) => stem.to_string_lossy().into_owned(),
                            None => {
                                eprintln!(
                                    "[ERROR] rename: Unable to extract title from {new_name}"
                                );
                                reply.error(libc::EINVAL);
                                return;
                            }
                        };

                        // Extension is required because our database stores the syntax field
                        // separately for use by the front-end application to determine file type
                        let syntax = match file_name_path.extension() {
                            Some(ext) => ext.to_string_lossy().into_owned(),
                            None => {
                                eprintln!(
                                    "[ERROR] rename: Cannot rename file without extension (e.g., {new_name}.txt, {new_name}.md)"
                                );
                                reply.error(libc::EINVAL);
                                return;
                            }
                        };

                        // Update note with new title and syntax
                        match self.db.update_note(
                            &note_id,
                            &title,
                            note.abstract_text.as_deref(),
                            &note.content,
                            &syntax,
                        ) {
                            Ok(_success) => {
                                // Update note parent if moving to different directory
                                match self
                                    .db
                                    .update_note_parent(&note_id, new_parent_id.as_deref())
                                {
                                    Ok(_success) => {
                                        self.update_inode_mappings(&old_path, &new_path);
                                        reply.ok();
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "[ERROR] rename: Failed to update note parent: {e}"
                                        );
                                        reply.error(libc::EIO);
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("[ERROR] rename: Failed to update note: {e}");
                                reply.error(libc::EIO);
                            }
                        }
                    }
                    Ok(None) => {
                        eprintln!(
                            "[ERROR] rename: Note with id {note_id} not found in database"
                        );
                        reply.error(ENOENT);
                    }
                    Err(e) => {
                        eprintln!(
                            "[ERROR] rename: Database error retrieving note {note_id}: {e}"
                        );
                        reply.error(ENOENT);
                    }
                }
            }
            Ok(None) => {
                // Neither a directory nor a note - doesn't exist
                eprintln!("[DEBUG] rename: Path {old_path} not found in database");
                reply.error(ENOENT);
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] rename: Database error checking for note {old_path}: {e}"
                );
                reply.error(ENOENT);
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
        // Check the filename
        let filename = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        // Handle special editor files (backup, swap, temporary files)
        if Self::is_editor_temp_file(filename) {
            // For editor temporary files, just reply OK without doing anything
            // This allows editors like Neovim to create and delete backup files
            reply.ok();
            return;
        }

        // Get parent path
        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Construct the full path
        let path = if parent_path == "/" {
            format!("/{filename}")
        } else {
            format!("{parent_path}/{filename}")
        };

        // Normalize path for database queries
        let db_path = Self::normalize_path_for_db(&path);

        // Get the id

        let id = match self.db.get_note_id_by_path(db_path) {
            Ok(maybe_id) => match maybe_id {
                Some(id) => id,
                None => {
                    eprintln!("74 [ERROR] (fn open) Could not find id for {path}");
                    reply.error(ENOENT);
                    return;
                }
            },
            Err(e) => {
                eprintln!("75 [ERROR] (fn open) Could not find id for {path}: {e}");
                reply.error(ENOENT);
                return;
            }
        };

        if self.is_dir(&path) {
            // This note has children, so it's a directory - cannot delete as a file
            reply.error(libc::EISDIR);
            return;
        }

        match self.db.delete_note(&id) {
            Ok(_) => {
                // Successfully deleted the note
                let file_path = if parent_path == "/" {
                    format!("/{filename}")
                } else {
                    format!("{parent_path}/{filename}")
                };

                if let Some(inode) = self.inode_map.remove(&file_path) {
                    self.reverse_inode_map.remove(&inode);
                }
            }
            Err(_) => {
                // If not successful
                reply.error(libc::EIO);
                return;
            }
        }
        reply.ok();
    }

    /// Only required in linux kernel before 2.6
    /// Otherwise the kernel will call open and create
    fn mknod(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
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

        let parent_id = if parent_path == "/" {
            None
        } else {
            let db_parent_path = Self::normalize_path_for_db(&parent_path);
            match self.db.get_folder_id_by_path(db_parent_path, self.user_id.as_str()) {
                Ok(maybe_id) => maybe_id,
                Err(e) => {
                    eprintln!(
                        "76 [ERROR] (fn open) Unable to query database for id for the directory {parent_path}: {e}"
                    );
                    reply.error(ENOENT);
                    return;
                }
            }
        };
        // Create the candidate full path
        let full_path = if parent_path == "/" {
            format!("/{file_name}")
        } else {
            format!("{parent_path}/{file_name}")
        };

        // Get the title and extension

        let file_name_path = Path::new(file_name);
        let base = file_name_path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned());
        let ext = file_name_path
            .extension()
            .map(|e| e.to_string_lossy().into_owned());

        let title = match base {
            Some(t) => t,
            None => {
                eprintln!("77 [ERROR] (fn mknod) Unable to get stem from {file_name}");
                reply.error(ENOENT);
                return;
            }
        };
        let syntax = match ext {
            Some(s) => s,
            None => {
                eprintln!(
                    "78 [ERROR] All files in this filesystem must have an extension (e.g., {file_name}.txt, {file_name}.md)"
                );
                reply.error(libc::EINVAL);
                return;
            }
        };

        let content = "";
        let abstract_text = Some("");
        let id = format!("{:x}", uuid::Uuid::new_v4().as_simple());
        let _id = match self.db.create_note(
            &id,
            &title,
            abstract_text,
            content,
            &syntax,
            parent_id.as_deref(),
            self.user_id.as_str(),
        ) {
            // Get the returned id in case the API changes
            Ok(id) => id,
            Err(e) => {
                eprintln!("79 [ERROR] Unable to create note for {full_path}: {e}");
                reply.error(ENOENT);
                return;
            }
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
            perm: (mode & 0o777) as u16,
            nlink: 1,
            uid: 501,
            gid: 20,
            rdev: 0,
            flags: 0,
            blksize: 512,
        };

        reply.entry(&TTL, &attr, 0);
    }

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

        // Construct the path of the directory to delete
        let path = if parent_path == "/" {
            format!("/{dirname}")
        } else {
            format!("{parent_path}/{dirname}")
        };

        // Get the folder ID of the directory being deleted
        let db_path = Self::normalize_path_for_db(&path);
        let folder_id = match self.db.get_folder_id_by_path(db_path, self.user_id.as_str()) {
            Ok(Some(id)) => id,
            Ok(None) => {
                eprintln!("[ERROR] rmdir: Folder {path} not found");
                reply.error(ENOENT);
                return;
            }
            Err(e) => {
                eprintln!("[ERROR] rmdir: Database error looking up folder {path}: {e}");
                reply.error(ENOENT);
                return;
            }
        };

        // NOTE CASCADE on a Foreign Key would be nice here
        let has_children = match self.db.get_child_count(Some(&folder_id), self.user_id.as_str()) {
            Ok((fc, nc)) => nc + fc > 0,
            Err(e) => {
                eprintln!("82 [ERROR] (fn rmdir) Unable to get child counts from database");
                eprintln!("{e}");
                reply.error(ENOENT);
                return;
            }
        };

        if has_children {
            reply.error(libc::EIO);
            return;
        }

        // Directory is empty, proceed with deletion
        match self.db.delete_folder(&folder_id, self.user_id.as_str()) {
            Ok(success) => {
                if success {
                    // Successfully deleted the directory
                    // Remove from inode mappings
                    if let Some(inode) = self.inode_map.remove(&path) {
                        self.reverse_inode_map.remove(&inode);
                    }
                    reply.ok();
                } else {
                    eprintln!(
                        "[ERROR] rmdir: Unable to delete directory {path} with id {folder_id}"
                    );
                    reply.error(ENOENT);
                }
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] rmdir: SQL error trying to delete directory {path} with id {folder_id}: {e}"
                );
                reply.error(ENOENT);
            }
        }
    }
}
