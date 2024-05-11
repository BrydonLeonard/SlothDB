use crate::model::kv::KV;
use std::fs::File;
use std::io::{self, BufRead};

#[derive(Debug)]
pub enum TableErr {
    IO(String),
    KvMissing(String),
    BadFile(String),
}

const INDEX_FILE_SUFFIX: &str = ".index";
const DATA_FILE_SUFFIX: &str = ".data";

/// Writes the data from the given iterator to disk.
/// 
/// Index files consist of newline-delimited pairs of key:position, where position encodes both 
/// the position and length of each key's corresponding value.
///
/// The data files are just every value concatenated and written to disk as a string.
/// 
pub fn flush<'a>(file_name: &str, in_data: impl Iterator<Item = &'a KV>) -> Result<(), TableErr> {
    let index_file_name = format!("{}{}", file_name, INDEX_FILE_SUFFIX);
    let data_file_name = format!("{}{}", file_name, DATA_FILE_SUFFIX);
        
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
    match data_file_position(file_name, key)? {
        Some(_) => return Ok(true),
        None => return Ok(false),
    }
}

/// Reads the value for the given key.
/// TODO: This currently reads the whole file into memory. That's obviously
/// not what we want to be doing. We have the position of the value in the 
/// file, so skip straight there and read it.
pub fn read(file_name: &str, key: &str) -> Result<Option<String>, TableErr> {
    let Some(position) = data_file_position(file_name, key)? else {
        return Ok(None);
    };
    
    Ok(Some(read_at_position(file_name, position)?))
}

fn read_at_position(file_name: &str, position: DataPosition) -> Result<String, TableErr> {
    let data_file_name = format!("{}{}", file_name, DATA_FILE_SUFFIX);
    let data = std::fs::read_to_string(data_file_name);

    let start: usize = position.0.try_into().expect("Couldn't parse u32 into usize");
    let end: usize = (position.0 + position.1).try_into().expect("Couldn't parse u32 sum into usize");

    Ok(data?[start..end].to_string())
}

pub fn iterate_entries<'a>(file_name: &'a str) -> Result<impl Iterator<Item = Result<KV, TableErr>> + 'a, TableErr> {
    let index_file_name = format!("{}{}", file_name, INDEX_FILE_SUFFIX);
    
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

fn data_file_position(file_name: &str, key: &str) -> Result<Option<DataPosition>, TableErr> {
    let index_file_name = format!("{}{}", file_name, INDEX_FILE_SUFFIX);

    let index_file_reader = io::BufReader::new(File::open(index_file_name)?);

    for line in index_file_reader.lines() {
        let l = line?;

        if l.starts_with(key) {
            return Ok(Some(DataPosition::from_key(&l)?));
        }
    }

    Ok(None)
}

impl From<std::io::Error> for TableErr {
    fn from(error: std::io::Error) -> Self {
        return TableErr::IO(format!("Failed to open file: {:?}", error));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Once;

    static INIT: Once = Once::new();
    const TEST_FILE_NAME: &str = "test_files/disk_test";

    /// Initializes the test data and returns it in case a test wants to compare to it
    fn test_init() -> [KV; 5] {
        let data = [
            KV { key: String::from("foo"), value: String::from("fooble") },
            KV { key: String::from("bar"), value: String::from("barble") },
            KV { key: String::from("baz"), value: String::from("bazzle") },
            KV { key: String::from("raz"), value: String::from("razzle") },
            KV { key: String::from("daz"), value: String::from("dazzle") },
        ];

        INIT.call_once(|| {
            flush(TEST_FILE_NAME, data.iter()).expect("Failed to initialize test data");
        });

        return data;
    }

    #[test]
    fn flushes() -> Result<(), TableErr> {
        // Not the most exciting test, but if this one fails, then it makes it clear that
        // it's the flushing itself that's failing
        test_init();

        let data_file_contents = std::fs::read_to_string(format!("{}{}", TEST_FILE_NAME, ".data"))?;
        let index_file_contents = std::fs::read_to_string(format!("{}{}", TEST_FILE_NAME, ".index"))?;

        assert_eq!("fooblebarblebazzlerazzledazzle", data_file_contents);
        assert_eq!("foo:0,6\nbar:6,6\nbaz:12,6\nraz:18,6\ndaz:24,6", index_file_contents);

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

        assert_eq!("razzle", read(TEST_FILE_NAME, "raz")?.unwrap());

        Ok(())
    }

    #[test]
    fn iterates() -> Result<(), TableErr> {
        let data = test_init();

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
}

