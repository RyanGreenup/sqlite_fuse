use std::{
    collections::{HashMap, HashSet}, error::Error, path::PathBuf, time::{Duration, SystemTime, UNIX_EPOCH}
};

use chrono::Utc;
use chrono_tz::{Australia::Sydney, Tz};
const TIMEZONE: Tz = Sydney; // Australia/Sydney timezone

use fuser::Filesystem;

use libc::ENOENT;
use std::ffi::OsStr;

use fuser::{FileAttr, FileType, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request};

use crate::database::Database;

const TTL: Duration = Duration::from_secs(1); // 1 second

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
        if path == "/" {
            return true;  // Root is always a directory
        }

        let id = self.db.get_id_from_path(path);
        if let Some(id) = id {
            return self.db.get_child_count(&id) > 0;
        } else {
            return false;
        }
    }

    fn get_content(&self, path: &str) -> Option<String> {
        if path == "/" {
            return None;  // Root directory has no content
        }

        let id = self.db.get_id_from_path(path);
        if let Some(id) = id {
            self.db.get_content(&id)
        } else {
            // Only warn for paths that aren't system files
            if !Self::is_system_file(path) {
                eprintln!("[WARNING] Unable to get id for {path}");
            }
            return None;
        }
    }

    fn content_size(&self, path: &str) -> usize {
        match self.get_content(path) {
            Some(text) => text.len(),
            None => {
                if self.is_dir(path) {
                    // Directories have no content, which is expected
                    0
                } else if !Self::is_system_file(path) {
                    eprintln!("[WARNING] No content found for file {path}");
                    0
                } else {
                    // System files that don't exist - no warning needed
                    0
                }
            }
        }
    }

    fn split_parent_path_and_filename(path: &str) -> (String, String) {
        let (parent_path, filename) = if let Some(pos) = path.rfind('/') {
            let parent = &path[..pos];
            let name = &path[pos + 1..];
            (if parent.is_empty() { "/" } else { parent }, name)
        } else {
            ("/", &path[..])
        };
        return (parent_path.to_string(), filename.to_string());
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

    fn get_children(&self, path: &str) -> Vec<(bool, String)> {
        let mut results: Vec<(bool, String)> = vec![];

        if path == "/" {
            // For root path, get items with no parent
            for item in self.db.get_all().values() {
                if item.parent_id.is_none() {
                    let count = self.db.get_child_count(&item.id);
                    let is_dir = count > 0;
                    results.push((is_dir, item.title.clone()));
                }
            }
        } else {
            match self.db.get_id_from_path(path) {
                Some(id) => {
                    for item in self.db.get_children(&id) {
                        let count = self.db.get_child_count(&item.id);
                        let is_dir = count > 0;
                        results.push((is_dir, item.title.clone()));
                    }
                }
                None => {
                    eprintln!("[WARNING] Could not find id for path {path}");
                    return vec![];
                }
            }
        }

        return results;
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

    pub fn get_parent_id_from_path(&self, path: &str) -> Option<String> {
        if let Some(id) = self.db.get_id_from_path(path) {
            if let Some(item) = self.db.get(&id) {
                return item.parent_id.clone();
            }
        } else {
            eprintln!("Unable to get ID from {path}");
        }
        None
    }

    pub fn get_parent_id_from_parent_path(
        &self,
        parent_path: &str,
    ) -> Result<Option<String>, Box<dyn Error>> {
        let parent_note_id = if parent_path == "/" {
            Ok(None)
        } else {
            match self.db.get_id_from_path(&parent_path) {
                Some(id) => Ok(Some(id)),
                None => {
                    // This is an error because the id couldn't be found
                    Err("Unable to get ID from parent path")?
                }
            }
        };
        return parent_note_id;
    }

    fn is_system_file(path: &str) -> bool {
        // Extract filename from path
        let filename = path.split('/').last().unwrap_or(path);

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

    fn current_timestamp() -> String {
        let utc_now = Utc::now();
        let sydney_time = utc_now.with_timezone(&TIMEZONE);
        sydney_time.format("%Y-%m-%d %H:%M:%S").to_string()
    }

    pub fn new() -> Result<Self, Box<dyn Error>> {
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
        let db = Database::new();

        let mut fs = ExampleFuseFs {
            db,
            inode_map: HashMap::new(),
            reverse_inode_map: HashMap::new(),
            next_inode: 2,
        };

        // Root directory gets inode 1
        fs.inode_map.insert("/".to_string(), 1);
        fs.reverse_inode_map.insert(1, "/".to_string());

        Ok(fs)
    }

}

impl Filesystem for ExampleFuseFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        // Get the Full Path
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        eprintln!("[DEBUG] lookup: parent={}, name={}", parent, name_str);

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

        // Check if this path exists in our database
        let path_exists = self.db.get_id_from_path(&full_path).is_some();
        eprintln!("[DEBUG] lookup: full_path={}, exists_in_db={}", full_path, path_exists);

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

        // Check if the file/directory exists in the database
        if !path_exists {
            eprintln!("[DEBUG] lookup: path {} not found in database, returning ENOENT", full_path);
            reply.error(ENOENT);
            return;
        }

        // Is it a directory
        let is_dir = self.is_dir(&full_path);

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
        eprintln!("[DEBUG] getattr: ino={}", ino);
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
        let (parent_path, _filename) = Self::split_parent_path_and_filename(&path);

        // Ensure we can Get parent note ID (None for root level)
        let _parent_note_id = match self.get_parent_id_from_parent_path(&parent_path) {
            Ok(id) => id,
            Err(e) => {
                eprintln!("{e}");
                reply.error(ENOENT);
                return;
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

        // Ignore directories
        if self.is_dir(&path) {
            return;
        }

        // get the content
        let content = self.get_content(&path);

        // Extract the filename and parent path

        if let Some(content) = content {
            let content_bytes = content.as_bytes(); //  note_result.1.as_bytes();
            let start = offset as usize;
            if start < content_bytes.len() {
                reply.data(&content_bytes[start..]);
            } else {
                reply.data(&[]);
            }
            return;
        } else {
            // A file where we cant find content is an error
            reply.error(ENOENT);
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

        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Determine parent inode
        let parent_ino = if path == "/" {
            1  // Root's parent is itself
        } else {
            // Find parent path and get its inode
            if let Some(pos) = path.rfind('/') {
                let parent_path = if pos == 0 { "/" } else { &path[..pos] };
                self.inode_map.get(parent_path).copied().unwrap_or(1)
            } else {
                1
            }
        };

        // Handle entries that should always be there
        let mut entries = vec![
            (ino, FileType::Directory, ".".to_string()),
            (parent_ino, FileType::Directory, "..".to_string()),
        ];

        // Get additional Entries from the database
        let mut seen_titles: HashSet<String> = std::collections::HashSet::new();
        for (is_dir, title) in self.get_children(&path) {
            if !seen_titles.contains(&title) {
                seen_titles.insert(title.clone());

                // Create full path for the child
                let child_path = if path == "/" {
                    format!("/{}", title)
                } else {
                    format!("{}/{}", path, title)
                };

                // Get or create inode for the child
                let child_ino = self.get_or_create_inode(&child_path);

                let file_type = if is_dir {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                };

                entries.push((child_ino, file_type, title.to_string()));
            }
        }

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            // i + 1 means the index of the next entry
            if reply.add(entry.0, (i + 1) as i64, entry.1, entry.2) {
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

        // Get parent ID - None for root, Some(id) for other paths
        let parent_id = if parent_path == "/" {
            None
        } else {
            match self.db.get_id_from_path(&parent_path) {
                Some(id) => Some(id),
                None => {
                    eprintln!("Unable to find id for the directory {parent_path}");
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // Create the note
        self.db.create(None, folder_name, parent_id.as_deref());

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

        eprintln!("[DEBUG] create called: parent={}, name={}, mode={:#o}, flags={:#x}",
                 parent, file_name, mode, flags);

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

        /*
        // Consider splitting the extension to store in a db field
        let path = Path::new(file_name);
        let base = path.file_stem().map(|s| s.to_string_lossy().into_owned());
        let ext = path.extension().map(|e| e.to_string_lossy().into_owned());
        */

        // Get parent ID - None for root, Some(id) for other paths
        let parent_id = if parent_path == "/" {
            None
        } else {
            match self.db.get_id_from_path(&parent_path) {
                Some(id) => Some(id),
                None => {
                    eprintln!("Cannot get id for parent path {parent_path}");
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        self.db.create(None, file_name, parent_id.as_deref());

        // Debug: verify the file was created
        let created_path = if parent_path == "/" {
            format!("/{file_name}")
        } else {
            format!("{parent_path}/{file_name}")
        };

        if self.db.get_id_from_path(&created_path).is_none() {
            eprintln!("[DEBUG] File {created_path} was not found after creation!");
            eprintln!("[DEBUG] Parent path: {parent_path}, parent_id: {:?}", parent_id);

            // List all items in database for debugging
            eprintln!("[DEBUG] All items in database:");
            for (id, item) in self.db.get_all() {
                eprintln!("  ID: {}, Title: {}, Parent: {:?}", id, item.title, item.parent_id);
            }
        }

        /*
        If this panics we could instead:
        // Get or create the note for this file
        let _note_id = match self.get_or_create_note(&parent_path, title, "", extension) {
            Ok(id) => id,
            Err(_) => {
                reply.error(libc::EIO);
                return;
            }
        };
        */

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
        eprintln!("[DEBUG] write called: ino={}, offset={}, data_len={}", ino, offset, data.len());
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
        let parent_note_id = match self.get_parent_id_from_parent_path(&parent_path) {
            Ok(id) => id,
            Err(e) => {
                eprintln!("{e}");
                reply.error(ENOENT);
                return;
            }
        };

        // Get the id of this note
        let id = match self.db.get_id_from_path(&path) {
            Some(id) => id,
            None => {
                eprintln!("[ERROR] Unable to get id for path: {}", path);
                eprintln!("[DEBUG] Looking for path components:");
                for component in path.split('/').filter(|s| !s.is_empty()) {
                    eprintln!("  - {}", component);
                }
                reply.error(ENOENT);
                return;
            }
        };

        // Confirm this is not a directory
        if self.is_dir(&path) {
            reply.error(libc::EISDIR);
            return;
        }

        // Get the content
        // TODO consider that every look up is expensive
        let content = self.get_content(&path).unwrap_or_default();

        // Handle the write operation
        let new_content = if offset == 0 {
            // Overwrite from the beginning
            String::from_utf8_lossy(data).to_string()
        } else {
            // Append or insert at offset
            let mut content_bytes = content.into_bytes();
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
        match self.db.update_content(&id, &new_content) {
            Ok(_) => {
                reply.written(data.len() as u32);
            }
            Err(e) => {
                eprintln!("Failed to update content: {}", e);
                reply.error(libc::EIO);
            }
        }
    }

    /// Handle file opening operations (unified schema)
    ///
    /// This method verifies that a file exists before allowing it to be opened.
    fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        eprintln!("[DEBUG] open: ino={}, flags={:#x}", ino, flags);

        // Check if O_CREAT flag is set (0x40 or 0x100)
        const O_CREAT: i32 = 0x40;
        if flags & O_CREAT != 0 {
            eprintln!("[DEBUG] open called with O_CREAT flag - file creation through open!");
        }

        // Verify that the inode exists and corresponds to a valid file
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Extract the filename and parent path for database verification
        let (parent_path, filename) = Self::split_parent_path_and_filename(&path);

        // Get the parent note ID (unified schema)
        let parent_note_id = match self.get_parent_id_from_parent_path(&parent_path) {
            Ok(id) => id,
            Err(e) => {
                eprintln!("{e}");
                reply.error(ENOENT);
                return;
            }
        };

        if !self.db.get_id_from_path(&path).is_some() {
            if !Self::is_system_file(&path) {
                eprintln!("[ERROR] Could not find {path} in the database, which is unexpected");
            }
            reply.error(ENOENT);
            return;
        }

        // A directory should be accessed as a directory, not a file
        if self.is_dir(&path) {
            reply.error(ENOENT)
        } else {
            reply.opened(ino, 0);
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

        // Handle regular files - strip extension and find note by title
        let parent_note_id = if parent_path == "/" {
            None
        } else {
            match self.db.get_id_from_path(parent_path) {
                Some(id) => Some(id),
                None => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // Extract the title from the filename (i.e. handle extensions etc.)
        let title = filename;
        let id = self
            .db
            .get_id_from_path(&path)
            .expect("[ERROR] could not get id for {path}");

        // Handle size changes (file truncation/extension)
        if let Some(new_size) = size {
            let current_content = match self.db.get_content(&id) {
                Some(content) => content,
                None => {
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

            // Write the new_content too
            // let new_content = String::from_utf8_lossy(&content_bytes).to_string();
            //
            self.db.update(&id, Some(title), parent_note_id.as_deref());
        }

        // Return updated file attributes
        let attr = FileAttr {
            ino,
            size: self.content_size(&id) as u64,
            blocks: self.content_size(&id).div_ceil(512) as u64,
            atime: UNIX_EPOCH,  // TODO: Parse created_at string from db
            mtime: UNIX_EPOCH,  // TODO: Parse updated_at string from db
            ctime: UNIX_EPOCH,  // TODO: Parse updated_at string from db
            crtime: UNIX_EPOCH, // TODO: Parse created_at string from db
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
            match self.db.get_id_from_path(&parent_path) {
                Some(id) => Some(id),
                None => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        let new_parent_note_id = if new_parent_path == "/" {
            None
        } else {
            match self.db.get_id_from_path(&new_parent_path) {
                Some(id) => Some(id),
                None => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // Handle regular file/directory renaming
        // Get the old path from the old parent and name
        let old_path = if parent_path == "/" {
            format!("/{old_name}")
        } else {
            format!("{parent_path}/{old_name}")
        };

        // Get the ID for the item being renamed
        let id = match self.db.get_id_from_path(&old_path) {
            Some(id) => id,
            None => {
                eprintln!("[WARNING] Cannot get id for {}", old_path);
                reply.error(ENOENT);
                return;
            }
        };

        let now = Self::current_timestamp();

        // handle extensions
        let new_title = new_name;

        // Update the note with new title, parent, and extension
        self.db
            .update(&id, Some(new_title), new_parent_note_id.as_deref());

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

        let path = "{parent_path}/{filename}";

        // Get the id
        let id = self
            .db
            .get_id_from_path(path)
            .expect("[WARNING] Unable to find id for {path}");

        if self.is_dir(&path) {
            // This note has children, so it's a directory - cannot delete as a file
            reply.error(libc::EISDIR);
            return;
        }
        self.db.delete(&id);

        // If not successful
        // reply.error(libc::EIO);
        // return
        // If that was successful
        let file_path = if parent_path == "/" {
            format!("/{filename}")
        } else {
            format!("{parent_path}/{filename}")
        };

        if let Some(inode) = self.inode_map.remove(&file_path) {
            self.reverse_inode_map.remove(&inode);
        }

        reply.ok();
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
    /// Handle mknod operations - another way files can be created
    /// Some systems use mknod instead of create for file creation
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
        eprintln!("[DEBUG] mknod called: parent={}, name={:?}, mode={:#o}",
                 parent, name, mode);

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

        // Get parent ID - None for root, Some(id) for other paths
        let parent_id = if parent_path == "/" {
            None
        } else {
            match self.db.get_id_from_path(&parent_path) {
                Some(id) => Some(id),
                None => {
                    eprintln!("Cannot get id for parent path {parent_path}");
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // Create the file in database
        self.db.create(None, file_name, parent_id.as_deref());

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

        // get the path
        let path = format!("{parent_path}/{dirname}");

        // get the id
        let id = self
            .db
            .get_id_from_path(&path)
            .expect("[ERROR] Unable to extract id from {path}");
        // NOTE CASCADE on a Foreign Key would be nice here

        // Get the children
        let children_ids: Vec<String> = self
            .db
            .get_children(&id)
            .into_iter()
            .map(|i| i.id.clone())
            .collect();

        if children_ids.len() == 0 {
            // Directory is empty, proceed with deletion
            self.db.delete(&id);
            // Ask for success

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
            reply.ok();
        } else {
            reply.error(libc::EIO);
            return;
        }
    }
}
