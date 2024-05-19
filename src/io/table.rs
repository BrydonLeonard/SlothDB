use crate::lsm::kv::KV;
use crate::lsm::merge_iter::{ MergeIter, MergeDecision, kv_merge };
use std::fs::File;
use std::io::{self, BufRead};
use std::num::ParseIntError;

#[derive(Debug)]
pub enum TableErr {
    IO(String),
    KeyNotFound(String),
    BadFile(String),
}

const INDEX_FILE_SUFFIX: &str = ".index";
const DATA_FILE_SUFFIX: &str = ".data";

pub fn merge_and_flush(left_file_name: &str, right_file_name: &str, new_file_name: &str) -> Result<(), TableErr> {
    let left_iter = iterate_entries(left_file_name)?;
    let right_iter = iterate_entries(right_file_name)?;

    let merge_iter = MergeIter::new(left_iter, right_iter, |left_result, right_result| {
        match (left_result, right_result) {
            (Err(_), _) => MergeDecision::Left(false),
            (_, Err(_)) => MergeDecision::Right(false),
            (Ok(left), Ok(right)) => kv_merge(left, right),
        }
    }).map(|result| { result.expect("") });

    let _ = flush(new_file_name, merge_iter);

    Ok(())
}

pub fn clean(file_name: &str) -> Result<(), TableErr> {
    std::fs::remove_file(index_fn(file_name))?;
    std::fs::remove_file(data_fn(file_name))?;

    Ok(())
}


/// Writes the data from the given iterator to disk.
/// 
/// Index files consist of newline-delimited pairs of key:position, where position encodes both 
/// the position and length of each key's corresponding value.
///
/// The data files are just every value concatenated and written to disk as a string.
/// 
pub fn flush<'a>(file_name: &str, in_data: impl IntoIterator<Item = KV>) -> Result<(), TableErr> {
    let index_file_name = index_fn(file_name);
    let data_file_name = data_fn(file_name);
        
    let mut out_data: Vec<String> = Vec::new();
    let mut out_index: Vec<String> = Vec::new();

    let mut position = 0;
    for datum in in_data {
        out_index.push(format!("{}:{},{}", datum.key, position, datum.value.len()));

        position = position + datum.value.len();
        out_data.push(datum.value.clone());
    }

    if let Err(data_write_error) = std::fs::write(data_file_name, out_data.join("")) {
        return Err(TableErr::IO(format!("Failed to write data file: {:?}", data_write_error)));
    }
    if let Err(index_write_error) = std::fs::write(index_file_name, out_index.join("\n")) {
        return Err(TableErr::IO(format!("Failed to write index file: {:?}", index_write_error)));
    }

    Ok(())
}

pub fn file_contains(file_name: &str, key: &str) -> Result<bool, TableErr> {
    println!("Checking whether {} contains {}", file_name, key);
    match data_file_position(file_name, key) {
        Ok(_) => return Ok(true),
        Err(TableErr::KeyNotFound(_)) => return Ok(false),
        Err(e) => Err(e),
    }
}

/// Reads the value for the given key.
/// TODO: This currently reads the whole file into memory. That's obviously
/// not what we want to be doing. We have the position of the value in the 
/// file, so skip straight there and read it.
pub fn read(file_name: &str, key: &str) -> Result<String, TableErr> {
    println!("Checking {:?} for {:?}", file_name, key);
    let position = data_file_position(file_name, key)?;
    
    read_at_position(file_name, position)
}

fn read_at_position(file_name: &str, position: DataPosition) -> Result<String, TableErr> {
    let data_file_name = data_fn(file_name);
    let data = std::fs::read_to_string(data_file_name);

    let start: usize = position.0.try_into().expect("Couldn't parse u32 into usize");
    let end: usize = (position.0 + position.1).try_into().expect("Couldn't parse u32 sum into usize");

    Ok(data?[start..end].to_string())
}

pub fn iterate_entries<'a>(file_name: &'a str) -> Result<impl Iterator<Item = Result<KV, TableErr>> + 'a, TableErr> {
    let index_file_name = index_fn(file_name);
    
    let index_reader = io::BufReader::new(File::open(index_file_name)?);
    
    Ok(index_reader.lines().map(|key_or_err| {
        // This is really inefficient for the moment. The idea is that read_at_position will get 
        // a faster implementation one day.
        let key_and_position = key_or_err?;
        let value = read_at_position(file_name, DataPosition::from_key(&key_and_position)?)?;

        // At this point, if the key's malformed, we would've returned an Err already.
        let key = String::from(key_and_position.split(":").collect::<Vec<&str>>()[0]);

        Ok(
            KV { 
                key,
                value,
            }
        )   
    }))
}

/// The position of data in the data file. First value is the start position, second is its
/// length
#[derive(Debug)]
struct DataPosition(u32, u32);

impl DataPosition {
    fn from_strings(position: &str, length: &str) -> Result<DataPosition, TableErr> {
        let Ok(position_val) = position.parse::<u32>() else {
            return Err(TableErr::BadFile(format!("The position '{}' is invalid", position)));
        };

        let Ok(length_val) = length.parse::<u32>() else {
            return Err(TableErr::BadFile(format!("The length '{}' is invalid", length)));
        };

        Ok(DataPosition(position_val, length_val)) 
    }

    fn from_key(key: &str) -> Result<DataPosition, TableErr> {
        let parts: Vec<&str> = key.split(":").collect();
        if parts.len() < 2 {
            return Err(TableErr::BadFile(format!("The key '{}' did not contain a corresponding position", key)));
        }

        let entries: Vec<&str> = parts[1].split(",").collect();

        if entries.len() < 2 {
            return Err(TableErr::BadFile(format!("The position/lenth string '{}' is malformed", parts[1])));
        }

        Self::from_strings(entries[0], entries[1])
    }
}

fn data_file_position(file_name: &str, key: &str) -> Result<DataPosition, TableErr> {
    let index_file_name = index_fn(file_name);
    
    println!("Index file name is {}", index_file_name);

    let index_file_reader = io::BufReader::new(File::open(index_file_name)?);

    for line in index_file_reader.lines() {
        let l = line?;

        if l.starts_with(key) {
            return Ok(DataPosition::from_key(&l)?);
        }
    }

    Err(TableErr::KeyNotFound(key.to_string()))
}

impl From<std::io::Error> for TableErr {
    fn from(error: std::io::Error) -> Self {
        return TableErr::IO(format!("Failed to open file: {:?}", error));
    }
}

impl From<ParseIntError> for TableErr {
    fn from(err: ParseIntError) -> Self {
        return TableErr::BadFile(format!("Failed to parse Int: {:?}", err));
    }
}

fn index_fn(name: &str) -> String {
    format!("{}{}", name, INDEX_FILE_SUFFIX)
}

fn data_fn(name: &str) -> String {
    format!("{}{}", name, DATA_FILE_SUFFIX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Once;

    static INIT: Once = Once::new();
    const TEST_FILE_NAME: &str = "test_files/disk_test";

    fn test_data() -> [KV; 5] {
        [
            KV { key: String::from("bar"), value: String::from("barble") },
            KV { key: String::from("baz"), value: String::from("bazzle") },
            KV { key: String::from("daz"), value: String::from("dazzle") },
            KV { key: String::from("foo"), value: String::from("fooble") },
            KV { key: String::from("raz"), value: String::from("razzle") },
        ]
    }


    /// Initializes the test data and returns it in case a test wants to compare to it
    fn test_init() {
        INIT.call_once(|| {
            let data = test_data();

            flush(TEST_FILE_NAME, data.into_iter()).expect("Failed to initialize test data");
        });
    }


    #[test]
    fn flushes() -> Result<(), TableErr> {
        // Not the most exciting test, but if this one fails, then it makes it clear that
        // it's the flushing itself that's failing
        test_init();

        let data_file_contents = std::fs::read_to_string(format!("{}{}", TEST_FILE_NAME, ".data"))?;
        let index_file_contents = std::fs::read_to_string(format!("{}{}", TEST_FILE_NAME, ".index"))?;

        assert_eq!("barblebazzledazzlefooblerazzle", data_file_contents);
        assert_eq!("bar:0,6\nbaz:6,6\ndaz:12,6\nfoo:18,6\nraz:24,6", index_file_contents);

        Ok(())
    }

    #[test]
    fn contains_works() -> Result<(), TableErr> {
        std::fs::write("test_files/test_contains.index", "and:0,1\nthe:1,1\nmome:2,8\nraths:10,7\noutgrabe:17,10")?;

        assert!(file_contains("test_files/test_contains", "and")?);
        assert!(file_contains("test_files/test_contains", "raths")?);
        assert!(file_contains("test_files/test_contains", "outgrabe")?);

        assert!(!file_contains("test_files/test_contains", "foo")?);
        Ok(())
    }

    #[test]
    fn reads() -> Result<(), TableErr> {
        test_init();

        assert_eq!("razzle", read(TEST_FILE_NAME, "raz")?);

        Ok(())
    }

    #[test]
    fn iterates() -> Result<(), TableErr> {
        test_init();
        let data = test_data();

        let iterator = iterate_entries(TEST_FILE_NAME)?;

        let mut index = 0;
        for kv in iterator {
            let input_kv = &data[index];
            let iter_kv = kv.unwrap();
            assert_eq!(input_kv.key, iter_kv.key);
            assert_eq!(input_kv.value, iter_kv.value);

            index += 1;
        }

        Ok(())
    }

    #[test]
    fn merges() -> Result<(), TableErr> {
        test_init();
        let test_data_2 = [
            KV { key: String::from("bang"), value: String::from("bangle") },
            KV { key: String::from("far"), value: String::from("farbing") },
        ];

        let _ = flush("test_files/test_data_2", test_data_2.into_iter());
        let _ = merge_and_flush(TEST_FILE_NAME, "test_files/test_data_2", "test_files/merged_data");

        let data_file_contents = std::fs::read_to_string(format!("{}{}", "test_files/merged_data", ".data"))?;
        let index_file_contents = std::fs::read_to_string(format!("{}{}", "test_files/merged_data", ".index"))?;

        assert_eq!("banglebarblebazzledazzlefarbingfooblerazzle", data_file_contents);
        assert_eq!("bang:0,6\nbar:6,6\nbaz:12,6\ndaz:18,6\nfar:24,7\nfoo:31,6\nraz:37,6", index_file_contents);
        Ok(())
    }


        
}

