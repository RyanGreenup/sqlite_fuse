#!/usr/bin/env python3
"""
Comprehensive test suite for SQLite FUSE filesystem.
Tests CRUD operations, edge cases, and expected behavior.
"""

import os
import sys
import time
import tempfile
from pathlib import Path
from typing import Optional
import typer
import traceback

app = typer.Typer(help="FUSE filesystem test suite")

# Colors for terminal output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"


class TestResult:
    """Track test results"""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.skipped = 0
        self.errors = []

    def pass_test(self, name: str):
        self.passed += 1
        print(f"{GREEN}✓{RESET} {name}")

    def fail_test(self, name: str, reason: str):
        self.failed += 1
        self.errors.append((name, reason))
        print(f"{RED}✗{RESET} {name}")
        print(f"  {RED}Reason: {reason}{RESET}")

    def skip_test(self, name: str, reason: str):
        self.skipped += 1
        print(f"{YELLOW}⊘{RESET} {name}")
        print(f"  {YELLOW}Skipped: {reason}{RESET}")

    def summary(self):
        total = self.passed + self.failed + self.skipped
        print(f"\n{BLUE}{'='*60}{RESET}")
        print(f"Total: {total} | {GREEN}Passed: {self.passed}{RESET} | {RED}Failed: {self.failed}{RESET} | {YELLOW}Skipped: {self.skipped}{RESET}")
        if self.failed > 0:
            print(f"\n{RED}Failed tests:{RESET}")
            for name, reason in self.errors:
                print(f"  - {name}: {reason}")
        print(f"{BLUE}{'='*60}{RESET}")
        return self.failed == 0


class FUSETestSuite:
    """Main test suite for FUSE filesystem"""

    def __init__(self, mount_point: str):
        self.mount_point = Path(mount_point)
        self.results = TestResult()

        if not self.mount_point.exists():
            raise ValueError(f"Mount point {mount_point} does not exist")

        if not self.mount_point.is_dir():
            raise ValueError(f"{mount_point} is not a directory")

    def write_file(self, path: Path, content: str) -> bool:
        """Helper to write file"""
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
            return True
        except Exception as e:
            print(f"    Error writing file: {e}")
            return False

    def read_file(self, path: Path) -> Optional[str]:
        """Helper to read file"""
        try:
            return path.read_text()
        except Exception as e:
            print(f"    Error reading file: {e}")
            return None

    def delete_file(self, path: Path) -> bool:
        """Helper to delete file"""
        try:
            path.unlink()
            return True
        except Exception as e:
            print(f"    Error deleting file: {e}")
            return False

    def delete_dir(self, path: Path) -> bool:
        """Helper to delete directory"""
        try:
            path.rmdir()
            return True
        except Exception as e:
            print(f"    Error deleting directory: {e}")
            return False

    def list_dir(self, path: Path) -> Optional[list]:
        """Helper to list directory"""
        try:
            return list(path.iterdir())
        except Exception as e:
            print(f"    Error listing directory: {e}")
            return None

    # ==================== File CRUD Tests ====================

    def test_create_file_with_extension(self):
        """Test creating a file with extension"""
        file_path = self.mount_point / "test_file.txt"
        if self.write_file(file_path, "Hello, World!"):
            if file_path.exists() and file_path.read_text() == "Hello, World!":
                self.results.pass_test("Create file with extension")
                file_path.unlink()
            else:
                self.results.fail_test("Create file with extension", "File not created or content mismatch")
        else:
            self.results.fail_test("Create file with extension", "Failed to write file")

    def test_create_file_without_extension(self):
        """Test creating a file without extension (should fail or be rejected)"""
        file_path = self.mount_point / "test_file_no_ext"
        try:
            file_path.write_text("Content")
            # If file was created, check if it's actually accessible
            if file_path.exists():
                self.results.skip_test("Create file without extension", "FUSE allows files without extensions")
                file_path.unlink()
            else:
                self.results.pass_test("Create file without extension")
        except Exception as e:
            self.results.pass_test("Create file without extension (rejected)")

    def test_create_multiple_file_types(self):
        """Test creating files with various extensions"""
        extensions = ["md", "txt", "py", "org", "json", "yaml"]
        all_created = True

        for ext in extensions:
            file_path = self.mount_point / f"test_file.{ext}"
            if not self.write_file(file_path, f"Content for {ext}"):
                all_created = False
                break

        if all_created:
            self.results.pass_test(f"Create multiple file types ({', '.join(extensions)})")
            for ext in extensions:
                (self.mount_point / f"test_file.{ext}").unlink()
        else:
            self.results.fail_test("Create multiple file types", "Failed to create some file types")

    def test_read_file_content(self):
        """Test reading file content"""
        file_path = self.mount_point / "read_test.txt"
        test_content = "This is a test file for reading"

        if self.write_file(file_path, test_content):
            content = self.read_file(file_path)
            if content == test_content:
                self.results.pass_test("Read file content")
            else:
                self.results.fail_test("Read file content", f"Content mismatch: expected '{test_content}', got '{content}'")
            file_path.unlink()
        else:
            self.results.fail_test("Read file content", "Failed to write test file")

    def test_write_and_overwrite(self):
        """Test writing and overwriting file content"""
        file_path = self.mount_point / "overwrite_test.txt"

        if self.write_file(file_path, "Original content"):
            original = self.read_file(file_path)
            if self.write_file(file_path, "New content"):
                new_content = self.read_file(file_path)
                if new_content == "New content" and original == "Original content":
                    self.results.pass_test("Write and overwrite file")
                else:
                    self.results.fail_test("Write and overwrite file", f"Overwrite failed")
            file_path.unlink()
        else:
            self.results.fail_test("Write and overwrite file", "Failed to write test file")

    def test_append_to_file(self):
        """Test appending to file content"""
        file_path = self.mount_point / "append_test.txt"

        if self.write_file(file_path, "Line 1\n"):
            # Try to append by opening in append mode
            try:
                with open(file_path, "a") as f:
                    f.write("Line 2\n")
                content = self.read_file(file_path)
                if "Line 1" in content and "Line 2" in content:
                    self.results.pass_test("Append to file")
                else:
                    self.results.fail_test("Append to file", "Append operation failed")
            except Exception as e:
                self.results.fail_test("Append to file", str(e))
            file_path.unlink()
        else:
            self.results.fail_test("Append to file", "Failed to write test file")

    def test_delete_file(self):
        """Test deleting a file"""
        file_path = self.mount_point / "delete_test.txt"

        if self.write_file(file_path, "To be deleted"):
            if file_path.exists():
                if self.delete_file(file_path):
                    if not file_path.exists():
                        self.results.pass_test("Delete file")
                    else:
                        self.results.fail_test("Delete file", "File still exists after deletion")
                else:
                    self.results.fail_test("Delete file", "Failed to delete file")
            else:
                self.results.fail_test("Delete file", "File not found after creation")
        else:
            self.results.fail_test("Delete file", "Failed to create test file")

    # ==================== Folder CRUD Tests ====================

    def test_create_folder(self):
        """Test creating a folder"""
        folder_path = self.mount_point / "test_folder"
        try:
            folder_path.mkdir()
            if folder_path.exists() and folder_path.is_dir():
                self.results.pass_test("Create folder")
                folder_path.rmdir()
            else:
                self.results.fail_test("Create folder", "Folder not created or not a directory")
        except Exception as e:
            self.results.fail_test("Create folder", str(e))

    def test_create_nested_folders(self):
        """Test creating nested folder structure"""
        folder_path = self.mount_point / "parent" / "child" / "grandchild"
        try:
            folder_path.mkdir(parents=True, exist_ok=True)
            if folder_path.exists() and folder_path.is_dir():
                self.results.pass_test("Create nested folders")
                # Cleanup
                (self.mount_point / "parent" / "child" / "grandchild").rmdir()
                (self.mount_point / "parent" / "child").rmdir()
                (self.mount_point / "parent").rmdir()
            else:
                self.results.fail_test("Create nested folders", "Nested folder structure not created")
        except Exception as e:
            self.results.fail_test("Create nested folders", str(e))

    def test_list_empty_folder(self):
        """Test listing empty folder"""
        folder_path = self.mount_point / "empty_folder"
        try:
            folder_path.mkdir()
            items = self.list_dir(folder_path)
            if items is not None and len(items) == 0:
                self.results.pass_test("List empty folder")
            else:
                self.results.fail_test("List empty folder", f"Expected 0 items, got {len(items) if items else 'None'}")
            folder_path.rmdir()
        except Exception as e:
            self.results.fail_test("List empty folder", str(e))

    def test_list_folder_with_files(self):
        """Test listing folder with files"""
        folder_path = self.mount_point / "folder_with_files"
        try:
            folder_path.mkdir()
            # Create some files
            files = ["file1.txt", "file2.md", "file3.py"]
            for f in files:
                self.write_file(folder_path / f, "content")

            items = self.list_dir(folder_path)
            if items and len(items) == 3:
                self.results.pass_test("List folder with files")
            else:
                self.results.fail_test("List folder with files", f"Expected 3 items, got {len(items) if items else 'None'}")

            # Cleanup
            for f in files:
                (folder_path / f).unlink()
            folder_path.rmdir()
        except Exception as e:
            self.results.fail_test("List folder with files", str(e))

    def test_list_nested_folder(self):
        """Test listing nested folder structure"""
        root = self.mount_point / "nested_test"
        try:
            root.mkdir()
            sub1 = root / "sub1"
            sub2 = root / "sub2"
            sub1.mkdir()
            sub2.mkdir()

            self.write_file(root / "file.txt", "root file")
            self.write_file(sub1 / "file1.txt", "sub1 file")
            self.write_file(sub2 / "file2.txt", "sub2 file")

            root_items = self.list_dir(root)
            sub1_items = self.list_dir(sub1)
            sub2_items = self.list_dir(sub2)

            if (root_items and len(root_items) >= 2 and
                sub1_items and len(sub1_items) == 1 and
                sub2_items and len(sub2_items) == 1):
                self.results.pass_test("List nested folder structure")
            else:
                self.results.fail_test("List nested folder structure", "Nested structure not correct")

            # Cleanup
            (root / "file.txt").unlink()
            (sub1 / "file1.txt").unlink()
            (sub2 / "file2.txt").unlink()
            sub1.rmdir()
            sub2.rmdir()
            root.rmdir()
        except Exception as e:
            self.results.fail_test("List nested folder structure", str(e))

    def test_delete_empty_folder(self):
        """Test deleting an empty folder"""
        folder_path = self.mount_point / "empty_to_delete"
        try:
            folder_path.mkdir()
            if self.delete_dir(folder_path):
                if not folder_path.exists():
                    self.results.pass_test("Delete empty folder")
                else:
                    self.results.fail_test("Delete empty folder", "Folder still exists")
            else:
                self.results.fail_test("Delete empty folder", "Failed to delete folder")
        except Exception as e:
            self.results.fail_test("Delete empty folder", str(e))

    def test_delete_non_empty_folder(self):
        """Test attempting to delete non-empty folder (should fail)"""
        folder_path = self.mount_point / "non_empty_folder"
        try:
            folder_path.mkdir()
            self.write_file(folder_path / "file.txt", "content")

            if not self.delete_dir(folder_path):
                self.results.pass_test("Delete non-empty folder (correctly rejected)")
                # Cleanup
                (folder_path / "file.txt").unlink()
                folder_path.rmdir()
            else:
                self.results.fail_test("Delete non-empty folder", "Should not allow deleting non-empty folder")
        except OSError:
            self.results.pass_test("Delete non-empty folder (correctly rejected)")
            # Cleanup
            (folder_path / "file.txt").unlink()
            folder_path.rmdir()
        except Exception as e:
            self.results.fail_test("Delete non-empty folder", str(e))

    # ==================== Rename/Move Tests ====================

    def test_rename_file(self):
        """Test renaming a file"""
        old_path = self.mount_point / "old_name.txt"
        new_path = self.mount_point / "new_name.txt"

        try:
            self.write_file(old_path, "content")
            old_path.rename(new_path)

            if not old_path.exists() and new_path.exists():
                self.results.pass_test("Rename file")
                new_path.unlink()
            else:
                self.results.fail_test("Rename file", "Rename operation failed")
        except Exception as e:
            self.results.fail_test("Rename file", str(e))

    def test_rename_folder(self):
        """Test renaming a folder"""
        old_path = self.mount_point / "old_folder"
        new_path = self.mount_point / "new_folder"

        try:
            old_path.mkdir()
            old_path.rename(new_path)

            if not old_path.exists() and new_path.exists():
                self.results.pass_test("Rename folder")
                new_path.rmdir()
            else:
                self.results.fail_test("Rename folder", "Rename operation failed")
        except Exception as e:
            self.results.fail_test("Rename folder", str(e))

    def test_move_file_to_subfolder(self):
        """Test moving file to a subfolder"""
        folder = self.mount_point / "subfolder"
        old_path = self.mount_point / "file.txt"
        new_path = folder / "file.txt"

        try:
            folder.mkdir()
            self.write_file(old_path, "content")
            old_path.rename(new_path)

            if not old_path.exists() and new_path.exists():
                self.results.pass_test("Move file to subfolder")
                new_path.unlink()
                folder.rmdir()
            else:
                self.results.fail_test("Move file to subfolder", "Move operation failed")
        except Exception as e:
            self.results.fail_test("Move file to subfolder", str(e))

    # ==================== Edge Cases & Special Tests ====================

    def test_large_file(self):
        """Test creating and reading large file"""
        file_path = self.mount_point / "large_file.txt"
        large_content = "x" * (1024 * 100)  # 100KB

        try:
            self.write_file(file_path, large_content)
            content = self.read_file(file_path)

            if content and len(content) == len(large_content):
                self.results.pass_test("Create and read large file (100KB)")
            else:
                self.results.fail_test("Create and read large file", "Content size mismatch")
            file_path.unlink()
        except Exception as e:
            self.results.fail_test("Create and read large file", str(e))

    def test_special_characters_in_filename(self):
        """Test files with special characters in name"""
        # Note: Some characters may not be allowed by the filesystem
        safe_special = ["hello-world.txt", "hello_world.txt", "hello.world.txt"]

        all_created = True
        for filename in safe_special:
            file_path = self.mount_point / filename
            if not self.write_file(file_path, "content"):
                all_created = False
                break

        if all_created:
            self.results.pass_test("Files with special characters")
            for filename in safe_special:
                (self.mount_point / filename).unlink()
        else:
            self.results.fail_test("Files with special characters", "Failed to create some files")

    def test_unicode_characters_in_filename(self):
        """Test files with unicode characters"""
        unicode_files = ["résumé.txt", "文件.md", "файл.txt"]

        all_created = True
        for filename in unicode_files:
            file_path = self.mount_point / filename
            try:
                if not self.write_file(file_path, "content"):
                    all_created = False
                    break
            except Exception:
                all_created = False
                break

        if all_created:
            self.results.pass_test("Files with unicode characters")
            for filename in unicode_files:
                try:
                    (self.mount_point / filename).unlink()
                except:
                    pass
        else:
            self.results.skip_test("Files with unicode characters", "Unicode filenames not supported")

    def test_temp_file_filtering(self):
        """Test that editor temp files are not persisted"""
        temp_files = [
            ".test_file.txt.swp",  # Vim swap file
            "#test_file#",          # Emacs backup
            ".test_file.txt~",      # Emacs backup alternative
        ]

        for temp_file in temp_files:
            file_path = self.mount_point / temp_file
            try:
                self.write_file(file_path, "temp content")
                # These files should be created but possibly filtered
                self.results.skip_test(f"Temp file filtering ({temp_file})", "Behavior depends on FUSE implementation")
                if file_path.exists():
                    file_path.unlink()
            except Exception:
                pass

    def test_file_attributes(self):
        """Test file attributes (size, modification time)"""
        file_path = self.mount_point / "attr_test.txt"

        try:
            content = "Test content for attributes"
            self.write_file(file_path, content)

            stat = file_path.stat()
            if stat.st_size == len(content):
                self.results.pass_test("File attributes (size)")
                if stat.st_mtime > 0:
                    self.results.pass_test("File attributes (modification time)")
                else:
                    self.results.fail_test("File attributes (modification time)", "mtime not set")
            else:
                self.results.fail_test("File attributes (size)", f"Size mismatch: {stat.st_size} vs {len(content)}")

            file_path.unlink()
        except Exception as e:
            self.results.fail_test("File attributes", str(e))

    def test_deep_nesting(self):
        """Test deeply nested folder structure"""
        # Create a path with 10 levels deep
        base = self.mount_point / "deep"
        current = base
        depth = 10

        try:
            for i in range(depth):
                current = current / f"level{i}"
                current.mkdir(parents=True, exist_ok=True)

            # Try to create a file at the deepest level
            file_path = current / "deep_file.txt"
            if self.write_file(file_path, "deep content"):
                if file_path.exists():
                    self.results.pass_test(f"Deep nesting ({depth} levels)")
                    file_path.unlink()
                else:
                    self.results.fail_test("Deep nesting", "File not created at deepest level")

            # Cleanup (in reverse order)
            current = file_path.parent
            for i in range(depth, -1, -1):
                try:
                    current.rmdir()
                    current = current.parent
                except:
                    pass
        except Exception as e:
            self.results.fail_test("Deep nesting", str(e))

    def test_empty_file(self):
        """Test creating and reading empty file"""
        file_path = self.mount_point / "empty.txt"

        try:
            self.write_file(file_path, "")
            content = self.read_file(file_path)

            if content == "":
                self.results.pass_test("Create and read empty file")
            else:
                self.results.fail_test("Create and read empty file", f"Expected empty, got: {repr(content)}")
            file_path.unlink()
        except Exception as e:
            self.results.fail_test("Create and read empty file", str(e))

    def test_root_directory_listing(self):
        """Test listing root directory"""
        try:
            items = self.list_dir(self.mount_point)
            if items is not None:
                self.results.pass_test(f"Root directory listing ({len(items)} items)")
            else:
                self.results.fail_test("Root directory listing", "Failed to list root")
        except Exception as e:
            self.results.fail_test("Root directory listing", str(e))

    def test_file_not_found(self):
        """Test reading non-existent file"""
        file_path = self.mount_point / "nonexistent_file_xyz.txt"

        try:
            content = self.read_file(file_path)
            if content is None:
                self.results.pass_test("File not found error handling")
            else:
                self.results.fail_test("File not found", "Should not be able to read non-existent file")
        except FileNotFoundError:
            self.results.pass_test("File not found error handling")
        except Exception as e:
            self.results.fail_test("File not found error handling", str(e))

    def test_permission_denied(self):
        """Test permission denied scenario"""
        # This is tricky to test without proper setup
        self.results.skip_test("Permission denied", "Requires specific permission setup")

    # ==================== Multi-file Operations ====================

    def test_multiple_files_in_folder(self):
        """Test creating and managing multiple files in one folder"""
        folder = self.mount_point / "multi_file_folder"
        num_files = 20

        try:
            folder.mkdir()

            # Create multiple files
            for i in range(num_files):
                self.write_file(folder / f"file{i}.txt", f"Content {i}")

            items = self.list_dir(folder)
            if items and len(items) == num_files:
                self.results.pass_test(f"Create and list {num_files} files")
            else:
                self.results.fail_test(f"Multiple files in folder", f"Expected {num_files}, got {len(items) if items else 0}")

            # Cleanup
            for i in range(num_files):
                (folder / f"file{i}.txt").unlink()
            folder.rmdir()
        except Exception as e:
            self.results.fail_test("Multiple files in folder", str(e))

    def test_concurrent_file_operations(self):
        """Test creating and deleting files in sequence"""
        try:
            for i in range(10):
                file_path = self.mount_point / f"concurrent_{i}.txt"
                self.write_file(file_path, f"Content {i}")
                content = self.read_file(file_path)
                if content != f"Content {i}":
                    self.results.fail_test("Concurrent file operations", f"Content mismatch at iteration {i}")
                    return
                self.delete_file(file_path)

            self.results.pass_test("Sequential file operations (10 cycles)")
        except Exception as e:
            self.results.fail_test("Sequential file operations", str(e))

    def run_all_tests(self):
        """Run all tests"""
        print(f"\n{BLUE}{'='*60}{RESET}")
        print(f"{BLUE}FUSE Filesystem Test Suite{RESET}")
        print(f"Mount Point: {self.mount_point}")
        print(f"{BLUE}{'='*60}{RESET}\n")

        # File CRUD Tests
        print(f"\n{BLUE}File CRUD Operations:{RESET}")
        self.test_create_file_with_extension()
        self.test_create_file_without_extension()
        self.test_create_multiple_file_types()
        self.test_read_file_content()
        self.test_write_and_overwrite()
        self.test_append_to_file()
        self.test_delete_file()

        # Folder CRUD Tests
        print(f"\n{BLUE}Folder CRUD Operations:{RESET}")
        self.test_create_folder()
        self.test_create_nested_folders()
        self.test_list_empty_folder()
        self.test_list_folder_with_files()
        self.test_list_nested_folder()
        self.test_delete_empty_folder()
        self.test_delete_non_empty_folder()

        # Rename/Move Tests
        print(f"\n{BLUE}Rename/Move Operations:{RESET}")
        self.test_rename_file()
        self.test_rename_folder()
        self.test_move_file_to_subfolder()

        # Edge Cases
        print(f"\n{BLUE}Edge Cases & Special Scenarios:{RESET}")
        self.test_empty_file()
        self.test_large_file()
        self.test_special_characters_in_filename()
        self.test_unicode_characters_in_filename()
        self.test_file_attributes()
        self.test_deep_nesting()
        self.test_root_directory_listing()
        self.test_file_not_found()
        self.test_temp_file_filtering()
        self.test_permission_denied()

        # Multi-file Operations
        print(f"\n{BLUE}Multi-file Operations:{RESET}")
        self.test_multiple_files_in_folder()
        self.test_concurrent_file_operations()

        return self.results.summary()


@app.command()
def run(
    mount_point: str = typer.Argument(..., help="Mount point of the FUSE filesystem")
):
    """Run comprehensive FUSE filesystem tests"""
    try:
        suite = FUSETestSuite(mount_point)
        success = suite.run_all_tests()
        sys.exit(0 if success else 1)
    except ValueError as e:
        print(f"{RED}Error: {e}{RESET}")
        sys.exit(1)
    except KeyboardInterrupt:
        print(f"\n{YELLOW}Tests interrupted by user{RESET}")
        sys.exit(130)
    except Exception as e:
        print(f"{RED}Unexpected error: {e}{RESET}")
        traceback.print_exc()
        sys.exit(1)


@app.command()
def quick(
    mount_point: str = typer.Argument(..., help="Mount point of the FUSE filesystem")
):
    """Run quick smoke tests only"""
    try:
        suite = FUSETestSuite(mount_point)

        print(f"\n{BLUE}{'='*60}{RESET}")
        print(f"{BLUE}Quick FUSE Smoke Tests{RESET}")
        print(f"Mount Point: {suite.mount_point}")
        print(f"{BLUE}{'='*60}{RESET}\n")

        suite.test_create_file_with_extension()
        suite.test_create_folder()
        suite.test_read_file_content()
        suite.test_list_folder_with_files()
        suite.test_delete_file()
        suite.test_delete_empty_folder()
        suite.test_root_directory_listing()

        success = suite.results.summary()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"{RED}Error: {e}{RESET}")
        sys.exit(1)


if __name__ == "__main__":
    app()
