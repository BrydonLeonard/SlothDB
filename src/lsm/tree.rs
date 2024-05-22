use crate::io::table;
use crate::io::table::TableErr;
use crate::lsm::kv::KV;
use crate::lsm::merge_iter::{ MergeIter, kv_merge, result_merge };
use std::collections::{ VecDeque, HashMap };
use std::fs;

pub struct LsmTree {
    name: String,
    levels: Vec<LsmLevel>,
}

#[derive(Debug)]
struct LsmLevel {
    id: String,
    count: u32,
    tables: VecDeque<u32>,
    max_size: u32,  
}

fn kv_merge_iter<T>(l: T, r: T) -> MergeIter<T, Result<KV, TableErr>> 
    where T : Iterator<Item = Result<KV, TableErr>> {
    
    MergeIter::new(l, r, |l, r| { result_merge(l, r, kv_merge) })
}

const LEVEL_SCALING_FACTOR: u32 = 1;

impl LsmTree {
    pub fn new(name: String) -> Result<LsmTree, TableErr> {
        Ok(LsmTree {
            name,
            levels: Vec::new(),
        })
    }

    pub fn add(&mut self, in_data: impl IntoIterator<Item = KV>) -> Result<(), TableErr> {
        if self.levels.len() == 0 {
            self.add_level();
        }
        let level = &mut self.levels[0];
        let new_table_name = level.new_table();

        table::flush(&new_table_name, in_data)
    }

    /// The merge part of an LSM Tree. This is pretty inefficiently implemented for now, but
    /// it'll do the job.
    ///
    /// For the next implementation - this will all be much less confusing if the tables were
    /// stored in a struct with all of the methods in [table] hanging off of it.
    fn compact(&mut self) -> Result<(), TableErr> {
        for level_index in 0..self.levels.len() {
            if !self.levels[0].full() {
                return Ok(())
            }
           
            // We need another level
            if level_index + 1 >= self.levels.len() {
                self.add_level();
            }
            
            // A little confusing, admittedly, but return the two tables to be merged 
            // and create a new table in the next level to write to. Pass all of those
            // to the merger to actually perform the merge.
            let compaction_candidates = self.levels[0].oldest().expect("Couldn't pull oldest from the old level");
            let destination = self.levels[level_index + 1].new_table();

            let _ = table::merge_and_flush(&compaction_candidates.0, &compaction_candidates.1, &destination)?; 
            
            let _ = table::clean(&compaction_candidates.0)?;
            let _ = table::clean(&compaction_candidates.1)?;
        };
        
        Ok(())
    }

    fn add_level(&mut self) {
        let new_index = self.levels.len();
        self.levels.push(LsmLevel { 
            id: format!("{}-{}", self.name, new_index),
            count: 0,
            tables: VecDeque::new(), 
            max_size: u32::try_from(new_index + 1).expect("Failed to convert usize -> u32") * LEVEL_SCALING_FACTOR 
        });
    }

    /// Loads a table from disk
    /// The abstraction isn't leak_ing_ here; it's leaked all over the floor and 
    /// I have no mop. Version two needs to encapsulate all of this _somewhere_.
    fn load(table_name: &str) -> Result<LsmTree, TableErr> {
        let files = Self::list_files(&table_name)?;

        // Map of level to min and max index. Because we compact from the beginning,
        // the remaining files will be contiguous.
        let mut levels: HashMap<i32, (i32, i32)> = HashMap::new();
        for file in files {
            if file.ends_with(".index") {
                let (level, index) = Self::parse_file_name(&file[0..(file.len() - 6)])?;

                println!("Got level/index: {:?}, {:?}", level, index);
                if !levels.contains_key(&level) {
                    levels.insert(level, (index, index));
                } else {
                    let current_value = levels[&level];
                    let lower = i32::min(current_value.0, index);
                    let upper = i32::max(current_value.1, index);
                    levels.insert(level, (lower, upper));
                }
            }
        }

        let mut lsm_levels: Vec<LsmLevel> = Vec::new();
        for level_index in 0..levels.keys().len() {
            println!("Populating level {:?}", level_index);

            let (min, max) = levels[&i32::try_from(level_index).expect("Failed to convert")];

            println!("  ({:?}, {:?})", min, max);
            let max_u32 = u32::try_from(max).expect("Failed to convert");
            let min_u32 = u32::try_from(min).expect("Failed to convert");
            let tables = VecDeque::from((min_u32..(max_u32+1)).collect::<Vec<_>>());

            println!("  Loaded tables: {:?}", tables);

            lsm_levels.push(LsmLevel {
                id: format!("{}-{}", table_name, level_index.to_string()),
                max_size: u32::try_from(level_index + 1).expect("Failed to convert usize -> u32") * LEVEL_SCALING_FACTOR,
                count: u32::try_from(max - min + 1).expect("Failed to convert"),
                tables: tables
            });
        }

        println!("Creating tree with table name {}", table_name.to_string());
        Ok(LsmTree { 
            name: table_name.to_string(),
            levels: lsm_levels,
        })
    }

    /// Parses the file name to find the level and index of a given database file
    /// File names look like `filename-level-name`
    fn parse_file_name(file_name: &str) -> Result<(i32, i32), TableErr> {
        let dash_indices: Vec<_> = file_name.match_indices("-").collect();
        if dash_indices.len () != 2 {
            return Err(TableErr::BadFile(String::from("File name should have level and index parts")));
        }
        
        println!("parsing file name {} with indices ({:?}, {:?})", file_name, dash_indices[0], dash_indices[1]);
        println!("  {:?}", &file_name[dash_indices[0].0 + 1..dash_indices[1].0]);
        println!("  {:?}", &file_name[dash_indices[1].0 + 1..file_name.len()]);
        let level_part = file_name[dash_indices[0].0 + 1..dash_indices[1].0].parse()?;
        let index_part = file_name[dash_indices[1].0 + 1..file_name.len()].parse()?;

        Ok((level_part, index_part))
    }
    
    fn list_files<'a>(table_name: &'a str) -> Result<impl Iterator<Item = String> + 'a, TableErr> {
        let (path_part, name_part) = if table_name.contains("/") {
            let slash_indices: Vec<_> = table_name.match_indices("/").collect();
            let last_slash = slash_indices[slash_indices.len() - 1].0;

            (&table_name[0..last_slash], &table_name[last_slash + 1..table_name.len() - 1])
        } else {
            ("./", table_name)
        };

        let paths = fs::read_dir(path_part)?;

        Ok(paths.map(|path| path.unwrap().path().display().to_string()).filter(move |path| path.contains(name_part)))
    }
}

/// Implemented by types that can read values for a key from _somewhere_
pub trait Scan {
    fn read(&self, key: &str) -> Result<String, TableErr>;
}

impl LsmLevel {
    fn table_names<'a>(&'a self) -> impl IntoIterator<Item = String> + 'a {
        let name = self.id.to_string();
        // Iterate backwards because we want to check the newest tables first
        self.tables.iter().map(move |index| { format!("{}-{}", name, index) })
    }

    fn full(&self) -> bool {
        self.tables.len() >= usize::try_from(self.max_size).expect("Failed to convert u32 -> usize")
    }

    fn oldest(&mut self) -> Result<(String, String), &'static str> {
        if self.tables.len() < 2 {
            return Err("Level is too small to compact from");
        }

        let first = self.tables.pop_front().expect("Failed to pop despite vec being large enough");
        let second = self.tables.pop_front().expect("Failed to pop despite vec being large enough");

        Ok((
            self.table_name(first),
            self.table_name(second),
        ))
    }

    fn new_table(&mut self) -> String {
        self.count += 1;
        self.tables.push_back(self.count);
        self.table_name(*self.tables.back().unwrap())
    }

    fn table_name(&self, index: u32) -> String {
        format!("{}-{}", self.id, index)
    }
}

impl Scan for LsmLevel {
    fn read(&self, key: &str) -> Result<String, TableErr> {
        println!("Checking level {:?}", &self.id);
        println!("  Level has {:?} tables", &self.table_names().into_iter().collect::<Vec<_>>().len());
        for lsm_table in self.table_names() {
            println!("Checking table {:?}", lsm_table);
            if table::file_contains(&lsm_table, key)? {
                return table::read(&lsm_table, key);
            }
        }

        Err(TableErr::KeyNotFound(key.to_string()))
    }
}

impl Scan for LsmTree {
    fn read(&self, key: &str) -> Result<String, TableErr> {
        println!("Checking levels: {:?}. This tree's name is {}", &self.levels, &self.name);
        for level in &self.levels {
            match level.read(key) {
                Ok(value) => return Ok(value),
                Err(e) => println!("{:?}", e),
            }
        }
        
        Err(TableErr::KeyNotFound(key.to_string()))
    }
}

#[cfg(test)]
mod test {
    use crate::lsm::tree::*;
    use std::fs;
    #[test]
    fn compacts() -> Result<(), TableErr> {
        let mut tree = LsmTree { 
            levels: Vec::new(),
            name: String::from("test_files/lsm_test"),
        };

        let _ = tree.add(vec![
                 KV { key: String::from("a"), value: 50.to_string() },
                 KV { key: String::from("c"), value: 10512.to_string() },
        ])?;

        let _ = tree.add(vec![
                 KV { key: String::from("b"), value: 12.to_string() },
                 KV { key: String::from("e"), value: 125.to_string() },
        ])?;

        let _ = tree.compact(); 

        let f = fs::read_to_string("test_files/lsm_test-1-1.data")?;

        let a_value = tree.read("a")?;
        let b_value = tree.read("b")?;

        assert_eq!("501210512125", f);
        assert_eq!(a_value, "50".to_string());
        assert_eq!(b_value, "12".to_string());

        Ok(())
    }
    
    #[test]
    fn loads() -> Result<(), TableErr> {
        let mut tree = LsmTree { 
            levels: Vec::new(),
            name: String::from("test_files/load_test"),
        };

        let _ = tree.add(vec![
                 KV { key: String::from("a"), value: 50.to_string() },
                 KV { key: String::from("c"), value: 10512.to_string() },
        ])?;

        let _ = tree.add(vec![
                 KV { key: String::from("b"), value: 12.to_string() },
                 KV { key: String::from("e"), value: 125.to_string() },
        ])?;

        let loaded_tree: LsmTree = LsmTree::load("test_files/load_test")?;

        let a_value = loaded_tree.read("a")?;
        let b_value = loaded_tree.read("b")?;

        assert_eq!(a_value, "50".to_string());
        assert_eq!(b_value, "12".to_string());


        Ok(())
    }
}
