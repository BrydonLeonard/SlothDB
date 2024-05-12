use crate::io::table;
use crate::io::table::TableErr;
use crate::lsm::kv::KV;
use crate::lsm::merge_iter::{ MergeIter, kv_merge, result_merge };
use std::collections::VecDeque;

struct LsmTree {
    name: String,
    levels: Vec<LsmLevel>,
}

struct LsmLevel {
    id: u32,
    tables: VecDeque<u32>,
}

const MAX_LEVEL_ID: u8 = 10;

fn kv_merge_iter<T>(l: T, r: T) -> MergeIter<T, Result<KV, TableErr>> 
    where T : Iterator<Item = Result<KV, TableErr>> {
    
    MergeIter::new(l, r, |l, r| { result_merge(l, r, kv_merge) })
}

impl LsmTree {
    /// The merge part of an LSM Tree. This is pretty inefficiently implemented for now, but
    /// it'll do the job.
    ///
    fn compact(&self) -> Result<(), TableErr> {
        //let new_level = LsmLevel {
        //    id: (self.levels[0].id + 1) % MAX_LEVEL_ID,
        //    tables: 0
        //};


        //candidate_list.chunks(2).map(|elems| {
        //    // build MergeIter
        //});

        //for table_names in candidate_list.chunks(2) {
        //    if table_names.len() != 2 {
        //        panic!("Can only compact even numbers of tables");
        //    }
        //    table::merge_and_flush(&table_names[0], &table_names[1], "test");
        //}

        //
        //Ok(())
        Ok(())
    }
}

/// Implemented by types that can read values for a key from _somewhere_
trait Scan {
    fn read(&self, key: &str) -> Result<String, TableErr>;
}

impl LsmLevel {
    fn table_names<'a>(&'a self) -> impl IntoIterator<Item = String> + 'a {
        let name = self.id.to_string();
        // Iterate backwards because we want to check the newest tables first
        self.tables.iter().map(move |index| { format!("{}-{}", name, index) })
    }
}

impl Scan for LsmLevel {
    fn read(&self, key: &str) -> Result<String, TableErr> {
        for lsm_table in self.table_names() {
            if table::file_contains(&lsm_table, key)? {
                return table::read(&lsm_table, key);
            }
        }

        Err(TableErr::KeyNotFound)
    }
}

impl Scan for LsmTree {
    fn read(&self, key: &str) -> Result<String, TableErr> {
        for level in &self.levels {
            if let Ok(value) = level.read(key) {
                return Ok(value);
            }
        }
        
        Err(TableErr::KeyNotFound)
    }
}
