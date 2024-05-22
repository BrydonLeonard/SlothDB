use crate::lsm::tree::LsmTree;
use crate::lsm::kv::KV;
use crate::io::table::TableErr;
use crate::lsm::tree::Scan;

pub struct Client {
    mem_table: Vec<KV>,
    lsm_tree: LsmTree,
    max_size: usize,
}

#[derive(Debug)]
enum BinSearchErr {
    SMALLER, // value is smaller than the entire list
    LARGER, // value is larger than the entire list
    MISSING(usize), // value is missing. The contained index is the largest one smaller than
                    // the element
    EMPTY, // the list is empty
}

impl Client {
    pub fn new(db_name: &str) -> Result<Client, TableErr> {
        return Ok(
            Client {
                mem_table: Vec::new(),
                lsm_tree: LsmTree::new(db_name.to_string())?,
                max_size: 10,
            }
        )
    }

    fn put(&mut self, key: String, value: String) -> Result<(), TableErr> {
        let new_elem = KV {
            key: key.to_string(),
            value
        };

        match self.find_index(&key) {
            Ok(index) => {
                self.mem_table.remove(index);
                self.mem_table.insert(index, new_elem);
            },
            Err(BinSearchErr::SMALLER) => self.mem_table.insert(0, new_elem),
            Err(BinSearchErr::LARGER) | Err(BinSearchErr::EMPTY) => self.mem_table.push(new_elem),
            Err(BinSearchErr::MISSING(index)) => self.mem_table.insert(index + 1, new_elem),
        }


        if self.mem_table.len() >= self.max_size {
            let _ = self.lsm_tree.add(self.mem_table.clone());
            self.mem_table = Vec::new();
        }


        Ok(())
    }

    fn get(&self, key: &str) -> Result<String, TableErr> {
        if let Ok(index) = self.find_index(key) {
            return Ok(self.mem_table[index].value.to_string())
        } else {
            if let Ok(val) = self.lsm_tree.read(key) {
                return Ok(val);
            } else {
                return Err(TableErr::KeyNotFound(format!("DB does not contain {}", key)))
            }
        };
    }


    // Returns the index of the given key or the index of the largest element smaller
    // than they key
    fn find_index(&self, key: &str) -> Result<usize, BinSearchErr> {
        if self.mem_table.len() < 1 {
            return Err(BinSearchErr::EMPTY);
        }

        let mut low = 0;
        let mut high = self.mem_table.len() - 1;
        let mut mid = (high + low) / 2;

        let key_str = key.to_string();

        if self.mem_table[low].key > key_str {
            return Err(BinSearchErr::SMALLER);
        }

        if self.mem_table[high].key < key_str { 
            return Err(BinSearchErr::LARGER);
        }

        while low < high && self.mem_table[mid].key != key_str {
            if self.mem_table[mid].key < key_str {
                low = mid + 1;
            } else {
                high = if mid > 0 {
                    mid - 1
                } else {
                    mid
                }
            }
            mid = (high + low) / 2;
        }

        if self.mem_table[mid].key == key_str {
            return Ok(mid);
        }

        Err(BinSearchErr::MISSING(mid))
    }
}

#[cfg(test)]
mod test {
    use crate::db::client::*;
    #[test]
    fn str_eq() {
        let s = "foo".to_string();
        let s2 = "foo";
        assert_eq!(s, s2);
    }

    #[test]
    fn put_get() -> Result<(), TableErr> {
        let mut instance = Client::new("client-test")?;

        let test_elems: [KV; 4] = [
                KV { key: String::from("foo"), value: String::from("bar") },
                KV { key: String::from("egg"), value: String::from("baz") },
                KV { key: String::from("mome"), value: String::from("rath") },
                KV { key: String::from("wibbly"), value: String::from("wobbly") },
            ];

        for elem in test_elems {
            let _ = instance.put(elem.key.to_string(), elem.value.to_string());
            assert_eq!(instance.get(&elem.key[..])?, elem.value);
        }

        Ok(())
    }
    
    #[test]
    fn get_index() -> Result<(), BinSearchErr> {
        println!("Starting get_index");
        let mut instance = Client::new("client-test").expect("Failed to build client");

        let test_elems: [KV; 4] = [
                KV { key: String::from("foo"), value: String::from("bar") },
                KV { key: String::from("egg"), value: String::from("baz") },
                KV { key: String::from("mome"), value: String::from("rath") },
                KV { key: String::from("wibbly"), value: String::from("wobbly") },
            ];

        for elem in test_elems {
            let _ = instance.put(elem.key.to_string(), elem.value.to_string());
        }

        println!("Starting assertions");
        assert_eq!(0, instance.find_index("egg")?);
        assert_eq!(1, instance.find_index("foo")?);
        assert_eq!(2, instance.find_index("mome")?);
        assert_eq!(3, instance.find_index("wibbly")?);
        if let Err(BinSearchErr::MISSING(_)) = instance.find_index("gumgum") {
            // Happy case
        } else {
            panic!("Expected a MISSING response");
        }

        Ok(())
    }

    #[test]
    fn flushes_to_disk() -> Result<(), TableErr> {
        // TODO Make this work
        let mut instance = Client::new("test_files/client-flush-test").expect("Failed to build client");

        for i in 0..20 {
            instance.put(
                i.to_string(),
                i.to_string(),
            )?;

            assert_eq!(instance.get(&i.to_string())?, i.to_string());
        }

        Ok(())
    }
}
