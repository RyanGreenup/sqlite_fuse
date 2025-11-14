use std::collections::HashMap;
use uuid::Uuid;

pub struct Database {
    pub items: HashMap<String, Item>,
}
#[derive(Clone)]
pub struct Item {
    pub title: String,
    pub id: String,
    pub parent_id: Option<String>,
}

impl Item {
    fn new(id: &str, title: &str, parent_id: Option<&str>) -> Self {
        return Self {
            title: title.to_string(),
            id: id.to_string(),
            parent_id: parent_id.map(|s| s.to_string()),
        };
    }
}

fn generate_uuid() -> String {
    Uuid::new_v4().to_string()
}

impl Database {
    // Init
    pub fn new() -> Self {
        return Self {
            items: HashMap::new(),
        };
    }
    // Create
    pub fn create(&mut self, id: Option<&str>, title: &str, parent_id: Option<&str>) {
        let id = match id {
            Some(id) => id,
            None => &generate_uuid(),
        };
        let new_item = Item::new(id, title, parent_id);
        self.items.insert(id.to_string(), new_item);
    }
    // Read
    pub fn get(&self, id: &str) -> Option<&Item> {
        self.items.get(id)
    }
    pub fn get_all(&self) -> &HashMap<String, Item> {
        &self.items
    }
    // Update
    pub fn update(&mut self, id: &str, title: Option<&str>, parent_id: Option<&str>) {
        // Get the current Value
        match self.items.get(id) {
            Some(current) => {
                let mut current = current.clone();
                if let Some(title) = title {
                    current.title = title.to_string();
                }
                current.parent_id = parent_id.map(|s| s.to_string());
                self.items.insert(id.to_string(), current.clone());
            }
            None => {
                let title = title.expect("Cannot Update Note that doesn't exist, failed to create new item as title is None");
                self.create(Some(id), title, parent_id);
            }
        }
    }
    /// Deletes an Item from the database
    /// Returns the item like pop
    pub fn delete(&mut self, id: &str) -> Option<Item> {
        self.items.remove(id)
    }

    pub fn get_child_count(&self, id: &str) -> u32 {
        let mut count = 0;
        for item in self.items.values() {
            if item.parent_id.as_ref() == Some(&id.to_string()) {
                count += 1;
            }
        }
        count
    }

    pub fn get_children(&self, id: &str) -> Vec<&Item> {
        let mut children = vec![];
        for item in self.items.values() {
            if let Some(parent_id) = &item.parent_id {
                if *parent_id == id {
                    children.push(item);
                }
            }
        }
        children
    }

    fn get_parent_title(&self, id: &str) -> Option<String> {
        self.get(id).map(|item| item.title.clone())
    }

    pub fn get_path(&self, id: &str) -> String {
        let mut components = Vec::new();
        let mut current_id = Some(id.to_string());

        while let Some(ref id) = current_id {
            if let Some(item) = self.get(id) {
                components.push(item.title.clone());
                current_id = item.parent_id.clone();
            } else {
                break;
            }
        }

        components.reverse();
        components.join("/")
    }

    pub fn get_content(&self, id: &str) -> Option<String> {
        if self.get_child_count(id) > 0 {
            Some(String::from("Foo Bar Baz"))
        } else {
            None
        }
    }

    pub fn get_id_from_path(&self, path: &str) -> Option<String> {
        let components: Vec<&str> = path.split('/').collect();
        let mut current_id = None;

        for component in components {
            let mut found = false;
            for item in self.items.values() {
                if item.parent_id == current_id && item.title == component {
                    current_id = Some(item.id.clone());
                    found = true;
                    break;
                }
            }
            if !found {
                return None;
            }
        }

        current_id
    }

    pub fn is_path_dir(&self, path: &str) -> Option<bool> {
        let id = self.get_id_from_path(path);
        if let Some(id) = id {
            Some(self.get_child_count(&id) > 0)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_path_and_get_id_from_path_are_inverse() {
        let mut db = Database::new();

        // Create a hierarchy: root -> parent -> child
        db.create("root", "Root", None);
        db.create("parent", "Parent", Some("root"));
        db.create("child", "Child", Some("parent"));

        // Test that get_path and get_id_from_path are inverses
        let test_cases = vec!["root", "parent", "child"];

        for id in test_cases {
            let path = db.get_path(id);
            let recovered_id = db.get_id_from_path(&path);

            assert_eq!(
                recovered_id,
                Some(id.to_string()),
                "get_path and get_id_from_path should be inverses for id: {}",
                id
            );
        }

        // Test with a non-existent path
        assert_eq!(db.get_id_from_path("Root/NonExistent"), None);
        assert_eq!(db.get_id_from_path("NonExistent"), None);
    }
}
