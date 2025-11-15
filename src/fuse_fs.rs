use std::{
    collections::{HashMap, HashSet},
    error::Error,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const DEBUG: bool = true;

use chrono::Utc;
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
}

impl ExampleFuseFs {
    fn is_dir(&self, path: &str) -> bool {
        if path == "/" {
            return true; // Root is always a directory
        }
        match self.db.get_folder_id_by_path(path) {
            Ok(_id) => true,
            Err(_e) => false,
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

    pub fn new(connection: Connection, timezone: Option<Tz>) -> Result<Self, Box<dyn Error>> {
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

        let _id = match self.db.get_note_id_by_path(&full_path) {
            Ok(maybe_id) => match maybe_id {
                Some(id) => id,
                None => {
                    eprintln!("[ERROR] (fn open) Could not find id for {full_path}");
                    reply.error(ENOENT);
                    return;
                }
            },
            Err(e) => {
                eprintln!("[ERROR] (fn open) Could not find id for {full_path}: {e}");
                reply.error(ENOENT);
                return;
            }
        };

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
        let (_parent_path, _filename) = Self::split_parent_path_and_filename(&path);

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
            let id = match self.db.get_note_id_by_path(&path) {
                Ok(maybe_id) => match maybe_id {
                    Some(id) => id,
                    None => {
                        eprintln!("[ERROR] (fn open) Could not find id for {path}");
                        reply.error(ENOENT);
                        return;
                    }
                },
                Err(e) => {
                    eprintln!("[ERROR] (fn open) Could not find id for {path}: {e}");
                    reply.error(ENOENT);
                    return;
                }
            };

            let note = match self.db.get_note_by_id(&id) {
                Ok(maybe_note) => match maybe_note {
                    Some(note) => note,
                    None => {
                        eprintln!(
                            "[ERROR] (fn write) Unable to find note in database with id={id} {path}"
                        );
                        reply.error(ENOENT);
                        return;
                    }
                },
                Err(e) => {
                    eprintln!(
                        "[ERROR] (fn write) Unable to find search for in database with id={id} {path}"
                    );
                    eprintln!("{e}");

                    reply.error(ENOENT);
                    return;
                }
            };

            let size = note.content.len() as u64;
            let blocks = note.content.len().div_ceil(512) as u64;

            let attr = FileAttr {
                ino,
                size,
                blocks,
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

        // Get the id
        let id = match self.db.get_note_id_by_path(&path) {
            Ok(maybe_id) => match maybe_id {
                Some(id) => id,
                None => {
                    eprintln!("[ERROR] (fn open) Could not find id for {path}");
                    reply.error(ENOENT);
                    return;
                }
            },
            Err(e) => {
                eprintln!("[ERROR] (fn open) Could not find id for {path}: {e}");
                reply.error(ENOENT);
                return;
            }
        };

        // get the content
        let note = match self.db.get_note_by_id(&id) {
            Ok(maybe_note) => match maybe_note {
                Some(note) => note,
                None => {
                    eprintln!(
                        "[ERROR] (fn write) Unable to find note in database with id={id} {path}"
                    );
                    reply.error(ENOENT);
                    return;
                }
            },
            Err(e) => {
                eprintln!(
                    "[ERROR] (fn write) Unable to find search for in database with id={id} {path}"
                );
                eprintln!("{e}");
                reply.error(ENOENT);
                return;
            }
        };

        // get the content
        let content = note.content;

        // Extract the filename and parent path

        let content_bytes = content.as_bytes(); //  note_result.1.as_bytes();
        let start = offset as usize;
        if start < content_bytes.len() {
            reply.data(&content_bytes[start..]);
        } else {
            reply.data(&[]);
        }
        return;
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

        // Get the folder id
        let id = match self.db.get_folder_id_by_path(&path) {
            Ok(maybe_id) => match maybe_id {
                Some(id) => id,
                None => {
                    eprintln!("Unable to get folder for {path}");
                    reply.error(ENOENT);
                    return;
                }
            },
            Err(e) => {
                eprintln!("Unable to get folder for {path}: {e}");
                reply.error(ENOENT);
                return;
            }
        };

        // Determine parent inode
        let parent_ino = if path == "/" {
            1 // Root's parent is itself
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
        // TODO this should be a command line argument
        // TODO the underlying database should filter this for every query
        let todo_user_id = "84a9e6d1ba7f6fd229c4276440d43886";
        let db_titles = match self.db.get_folder_contents_recursive(&id, todo_user_id) {
            Ok(titles) => titles,
            Err(e) => {
                eprintln!("[ERROR] Unable to get titles for id = {id} at {path}: {e}");
                reply.error(ENOENT);
                return;
            }
        };

        for file in db_titles {
            let (is_dir, path) = match file {
                crate::database::FileType::File { path } => (false, path),
                crate::database::FileType::Directory { path } => (true, path),
            };

            // Path is like /foo/bar/baz, we need just baz
            let basename = path.split('/').last().unwrap_or(&path).to_string();

            if !seen_titles.contains(&basename) {
                seen_titles.insert(path.clone());

                // Get or create inode for the child
                let child_ino = self.get_or_create_inode(&path);

                let file_type = if is_dir {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                };

                entries.push((child_ino, file_type, path.to_string()));
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

    /// Handle directory creation operations
    ///
    /// Key behaviors:
    /// - Creates a note in the database that represents a directory
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
        // Construct the candidate full path (if successful)

        // Create the full path for the new directory
        let full_path = if parent_path == "/" {
            format!("/{folder_name}")
        } else {
            format!("{parent_path}/{folder_name}")
        };

        // Get parent ID - None for root, Some(id) for other paths
        let parent_id = if parent_path == "/" {
            None
        } else {
            match self.db.get_folder_id_by_path(&parent_path) {
                Ok(maybe_id) => maybe_id,
                Err(e) => {
                    eprintln!(
                        "[ERROR] Unable to query database for id for the directory {parent_path}"
                    );
                    eprintln!("{e}");
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // TODO the create method should take id as Option or not at all.
        let _id = match self.db.create_folder(folder_name, parent_id.as_deref()) {
            // Get the returned id in case the API changes
            Ok(id) => id,
            Err(e) => {
                eprintln!("[ERROR] Unable to create folder for {full_path}: {e}");
                reply.error(ENOENT);
                return;
            }
        };

        // Use the note_id (either existing or newly created) for further operations
        {
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
            "[DEBUG] create called: parent={}, name={}, mode={:#o}, flags={:#x}",
            parent, file_name, mode, flags
        );

        // Get parent path
        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Construct the candidate full_path
        let full_path = if parent_path == "/" {
            format!("/{file_name}")
        } else {
            format!("{parent_path}/{file_name}")
        };

        // Handle editor temporary files by creating them as regular empty files
        // but don't store them in the database
        if Self::is_editor_temp_file(file_name) {
            // Create a temporary inode for editor files but don't persist to database

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
                eprintln!("[ERROR] (fn setattr) Unable to get stem from {file_name}");
                reply.error(ENOENT);
                return;
            }
        };
        let syntax = match ext {
            Some(s) => s,
            None => {
                eprintln!("[ERROR] (fn setattr) Unable to get stem from {file_name}");
                reply.error(ENOENT);
                return;
            }
        };

        // Get parent ID - None for root, Some(id) for other paths
        // Get parent ID - None for root, Some(id) for other paths
        let parent_id = if parent_path == "/" {
            None
        } else {
            match self.db.get_folder_id_by_path(&parent_path) {
                Ok(maybe_id) => maybe_id,
                Err(e) => {
                    eprintln!(
                        "[ERROR] Unable to query database for id for the directory {parent_path}"
                    );
                    eprintln!("{e}");
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // TODO the create method should take id as Option or not at all.
        let id = format!("{:x}", uuid::Uuid::new_v4().as_simple());
        let todo_user_id = "84a9e6d1ba7f6fd229c4276440d43886";
        let content = "";
        let abstract_text = Some("");
        let id = match self.db.create_note(
            &id,
            &title,
            abstract_text,
            content,
            &syntax,
            parent_id.as_deref(),
            todo_user_id,
        ) {
            // Get the returned id in case the API changes
            Ok(id) => id,
            Err(e) => {
                eprintln!("[ERROR] Unable to create note for {full_path}: {e}");
                reply.error(ENOENT);
                return;
            }
        };

        if DEBUG {
            // Get the id from the path we just created
            let created_id = match self.db.get_note_id_by_path(&full_path) {
                Ok(maybe_id) => match maybe_id {
                    Some(id) => id,
                    None => {
                        eprintln!("[ERROR] (fn open) Could not find id for {full_path}");
                        reply.error(ENOENT);
                        return;
                    }
                },
                Err(e) => {
                    eprintln!("[ERROR] (fn open) Could not find id for {full_path}: {e}");
                    reply.error(ENOENT);
                    return;
                }
            };

            // Get the path back from that id
            let created_path = match self.db.get_note_path_by_id(&created_id) {
                Ok(maybe_path) => match maybe_path {
                    Some(path) => path,
                    None => {
                        eprintln!(
                            "[ERROR] Unable to to find path for id={id} that was just created from {full_path}"
                        );
                        reply.error(ENOENT);
                        return;
                    }
                },
                Err(e) => {
                    eprintln!(
                        "[ERROR] Unable to retrieve path for id={id} that was just created from {full_path}"
                    );
                    eprintln!("{e}");

                    reply.error(ENOENT);
                    return;
                }
            };

            // assert equality
            if created_path != full_path {
                eprintln!(
                    "[ERROR] Created Path differs from Retrieved path for note just created:"
                );
                eprintln!("{created_path}");
                eprintln!("{full_path}");
            }
        }

        {
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
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        /*
        // Extract the filename and parent path
        let (parent_path, filename) = if let Some(pos) = path.rfind('/') {
            let parent = &path[..pos];
            let name = &path[pos + 1..];
            (if parent.is_empty() { "/" } else { parent }, name)
        } else {
            ("/", &path[..])
        };
        */

        // Check if it's a directory
        if self.is_dir(&path) {
            reply.error(libc::EISDIR);
            return;
        }

        // Get the id of the note
        // TODO consider a function to get the note from the path to save a second lookup
        let id = match self.db.get_note_id_by_path(&path) {
            Ok(maybe_id) => match maybe_id {
                Some(id) => id,
                None => {
                    eprintln!("[ERROR] Could not find id for {path}");
                    reply.error(ENOENT);
                    return;
                }
            },
            Err(e) => {
                eprintln!("[ERROR] Could not find id for {path}");
                eprintln!("{e}");

                reply.error(ENOENT);
                return;
            }
        };

        let note = match self.db.get_note_by_id(&id) {
            Ok(maybe_note) => match maybe_note {
                Some(note) => note,
                None => {
                    eprintln!(
                        "[ERROR] (fn write) Unable to find note in database with id={id} {path}"
                    );
                    reply.error(ENOENT);
                    return;
                }
            },
            Err(e) => {
                eprintln!(
                    "[ERROR] (fn write) Unable to find search for in database with id={id} {path}"
                );
                eprintln!("{e}");
                reply.error(ENOENT);
                return;
            }
        };

        let content = note.content.clone();

        // TODO don't bother retrieving the content if offset=0
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

        // TODO consider a fine grained function to update only the content
        match self.db.update_note(
            &id,
            &note.title,
            note.abstract_text.as_deref(),
            &new_content,
            &note.syntax,
        ) {
            Ok(_n_rows_changed) => {
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

        // Confirm the parent_id exists
        let _parent_id = if parent_path == "/" {
            None
        } else {
            match self.db.get_folder_id_by_path(&parent_path) {
                Ok(maybe_id) => maybe_id,
                Err(e) => {
                    eprintln!(
                        "[ERROR] (fn open) Unable to query database for id for the directory {parent_path}: {e}"
                    );
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // Get the full path
        let full_path = if parent_path == "/" {
            format!("/{filename}")
        } else {
            format!("{parent_path}/{filename}")
        };

        // Confirm the note exists
        let _id = match self.db.get_note_id_by_path(&full_path) {
            Ok(maybe_id) => match maybe_id {
                Some(id) => id,
                None => {
                    eprintln!("[ERROR] (fn open) Could not find id for {path}");
                    reply.error(ENOENT);
                    return;
                }
            },
            Err(e) => {
                eprintln!("[ERROR] (fn open) Could not find id for {path}: {e}");
                reply.error(ENOENT);
                return;
            }
        };

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
        let (_parent_path, filename) = if let Some(pos) = path.rfind('/') {
            let parent = &path[..pos];
            let name = &path[pos + 1..];
            (if parent.is_empty() { "/" } else { parent }, name)
        } else {
            ("/", &path[..])
        };

        /*
        // Handle regular files - strip extension and find note by title
        let parent_id = if parent_path == "/" {
            None
        } else {
            match self.db.get_folder_id_by_path(&parent_path) {
                Ok(maybe_id) => maybe_id,
                Err(e) => {
                    eprintln!(
                        "[ERROR] (fn setattr) Unable to query database for id for the directory {parent_path}: {e}"
                    );
                    reply.error(ENOENT);
                    return;
                }
            }
        };
        */

        let file_name_path = Path::new(filename);
        let base = file_name_path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned());
        let ext = file_name_path
            .extension()
            .map(|e| e.to_string_lossy().into_owned());
        let title = match base {
            Some(t) => t,
            None => {
                eprintln!("[ERROR] (fn setattr) Unable to get stem from {filename}");
                reply.error(ENOENT);
                return;
            }
        };
        let syntax = match ext {
            Some(s) => s,
            None => {
                eprintln!("[ERROR] (fn setattr) Unable to get stem from {filename}");
                reply.error(ENOENT);
                return;
            }
        };

        let id = match self.db.get_note_id_by_path(&path) {
            Ok(maybe_id) => match maybe_id {
                Some(id) => id,
                None => {
                    eprintln!("[ERROR] (fn open) Could not find id for {path}");
                    reply.error(ENOENT);
                    return;
                }
            },
            Err(e) => {
                eprintln!("[ERROR] (fn open) Could not find id for {path}: {e}");
                reply.error(ENOENT);
                return;
            }
        };

        let note = match self.db.get_note_by_id(&id) {
            Ok(maybe_note) => match maybe_note {
                Some(note) => note,
                None => {
                    eprintln!(
                        "[ERROR] (fn setattr) Unable to find note in database with id={id} {path}"
                    );
                    reply.error(ENOENT);
                    return;
                }
            },
            Err(e) => {
                eprintln!(
                    "[ERROR] (fn setattr) Unable to find search for in database with id={id} {path}"
                );
                eprintln!("{e}");
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

            // Write the new_content too
            // TODO should we bother with this?
            let new_content = String::from_utf8_lossy(&content_bytes).to_string();
            //
            let _success = match self.db.update_note(
                &id,
                &title,
                note.abstract_text.as_deref(),
                &new_content,
                &syntax,
            ) {
                Ok(is_success) => {
                    if is_success {
                        is_success
                    } else {
                        eprintln!("[ERROR] (fn setattr) Unable to update {path} with id={id}");
                        reply.error(ENOENT);
                        return;
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[ERROR] (fn setattr) Sql error trying to write to {path} with id={id}"
                    );
                    eprintln!("{e}");
                    reply.error(ENOENT);
                    return;
                }
            };
        }

        let size = note.content.len() as u64;
        let blocks = note.content.len().div_ceil(512) as u64;

        // Return updated file attributes
        let attr = FileAttr {
            ino,
            size,
            blocks,
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

        let new_parent_id = if parent_path == "/" {
            None
        } else {
            match self.db.get_folder_id_by_path(&new_parent_path) {
                Ok(maybe_id) => maybe_id,
                Err(e) => {
                    eprintln!(
                        "[ERROR] (fn open) Unable to query database for id for the directory {parent_path}: {e}"
                    );
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

        let new_path = if new_parent_path == "/" {
            format!("/{new_name}")
        } else {
            format!("{new_parent_path}/{new_name}")
        };

        // Get the id for the item being renamed
        let id = match self.db.get_note_id_by_path(&old_path) {
            Ok(maybe_id) => match maybe_id {
                Some(id) => id,
                None => {
                    eprintln!("[ERROR] (fn open) Could not find id for {old_path}");
                    reply.error(ENOENT);
                    return;
                }
            },
            Err(e) => {
                eprintln!("[ERROR] (fn open) Could not find id for {old_path}: {e}");
                reply.error(ENOENT);
                return;
            }
        };

        // Get the new title and extension
        let file_name_path = Path::new(new_name);
        let base = file_name_path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned());
        let ext = file_name_path
            .extension()
            .map(|e| e.to_string_lossy().into_owned());

        let title = match base {
            Some(t) => t,
            None => {
                eprintln!("[ERROR] (fn setattr) Unable to get stem from {new_name}");
                reply.error(ENOENT);
                return;
            }
        };
        let syntax = match ext {
            Some(s) => s,
            None => {
                eprintln!("[ERROR] (fn setattr) Unable to get stem from {new_name}");
                reply.error(ENOENT);
                return;
            }
        };

        // Get the current note content

        let note = match self.db.get_note_by_id(&id) {
            Ok(maybe_note) => match maybe_note {
                Some(note) => note,
                None => {
                    eprintln!(
                        "[ERROR] (fn write) Unable to find note in database with id={id} {new_path}"
                    );
                    reply.error(ENOENT);
                    return;
                }
            },
            Err(e) => {
                eprintln!(
                    "[ERROR] (fn write) Unable to find search for in database with id={id} {old_path}"
                );
                eprintln!("{e}");
                reply.error(ENOENT);
                return;
            }
        };

        // Update the note with new title and extension
        let _succeeded = match self.db.update_note(
            &id,
            &title,
            note.abstract_text.as_deref(),
            &note.content,
            &syntax,
        ) {
            Ok(is_success) => is_success,
            Err(e) => {
                eprintln!(
                    "[ERROR] (fn rename) Unable to update note with new title and extension id={id} oldpath={old_path} newpath={new_path}"
                );
                eprintln!("{e}");
                reply.error(ENOENT);
                return;
            }
        };
        let _succeeded = match self.db.update_note_parent(&id, new_parent_id.as_deref()) {
            Ok(is_success) => is_success,
            Err(e) => {
                eprintln!(
                    "[ERROR] (fn rename) Unable to update note parent id={id} oldpath={old_path} newpath={new_path}"
                );
                eprintln!("{e}");
                reply.error(ENOENT);
                return;
            }
        };

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

        // TODO should we consider getting this straight from the database?
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

        let id = match self.db.get_note_id_by_path(&path) {
            Ok(maybe_id) => match maybe_id {
                Some(id) => id,
                None => {
                    eprintln!("[ERROR] (fn open) Could not find id for {path}");
                    reply.error(ENOENT);
                    return;
                }
            },
            Err(e) => {
                eprintln!("[ERROR] (fn open) Could not find id for {path}: {e}");
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
            match self.db.get_folder_id_by_path(&parent_path) {
                Ok(maybe_id) => maybe_id,
                Err(e) => {
                    eprintln!(
                        "[ERROR] (fn open) Unable to query database for id for the directory {parent_path}: {e}"
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
                eprintln!("[ERROR] (fn setattr) Unable to get stem from {file_name}");
                reply.error(ENOENT);
                return;
            }
        };
        let syntax = match ext {
            Some(s) => s,
            None => {
                eprintln!("[ERROR] (fn setattr) Unable to get stem from {file_name}");
                reply.error(ENOENT);
                return;
            }
        };

        let content = "";
        let abstract_text = Some("");
        let id = format!("{:x}", uuid::Uuid::new_v4().as_simple());
        let todo_user_id = "84a9e6d1ba7f6fd229c4276440d43886";
        let _id = match self.db.create_note(
            &id,
            &title,
            abstract_text,
            content,
            &syntax,
            parent_id.as_deref(),
            todo_user_id,
        ) {
            // Get the returned id in case the API changes
            Ok(id) => id,
            Err(e) => {
                eprintln!("[ERROR] Unable to create note for {full_path}: {e}");
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

        // get the path
        let path = format!("{parent_path}/{dirname}");

        let maybe_parent_id = if parent_path == "/" {
            None
        } else {
            match self.db.get_folder_id_by_path(&parent_path) {
                Ok(maybe_id) => maybe_id,
                Err(e) => {
                    eprintln!(
                        "[ERROR] (fn open) Unable to query database for id for the directory {parent_path}: {e}"
                    );
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // usually parent_id can be Option, here we must have a folder to delete
        let parent_id = match maybe_parent_id {
            Some(id) => id,
            None => {
                eprintln!("[ERROR] There is no id associated with the folder {path}");
                reply.error(ENOENT);
                return;
            }
        };

        // NOTE CASCADE on a Foreign Key would be nice here
        let todo_user_id = "84a9e6d1ba7f6fd229c4276440d43886";
        let has_children = match self
            .db
            .get_child_count(Some(&parent_id), Some(todo_user_id))
        {
            Ok((fc, nc)) => nc + fc > 0,
            Err(e) => {
                eprintln!("[ERROR] (fn rmdir) Unable to get child counts from database");
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
        match self.db.delete_folder(&parent_id) {
            Ok(success) => {
                if success {
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
                    eprintln!(
                        "[ERROR] (fn rmdir) Unable to delete directory {parent_path} with id {parent_id}"
                    );
                    reply.error(ENOENT);
                    return;
                }
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] (fn rmdir) SQL error trying to delete directory {parent_path} with id {parent_id}"
                );
                eprintln!("{e}");
                reply.error(ENOENT);
                return;
            }
        }
    }
}
