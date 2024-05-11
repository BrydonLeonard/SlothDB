fn main() {

}

#[derive(Debug)]
struct KV {
    key: String,
    value: String,
}


mod table_management {
    use crate::KV;
    use std::fs::File;
    use std::io::{self, BufRead};

    #[derive(Debug)]
    enum TableErr {
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
    /// ```
    /// foo:0,4
    /// bar:4,2
    /// baz:6:u
    /// ```
    ///
    /// The data files are just every value concatenated and written to disk as a string.
    /// 
    fn flush<'a>(file_name: &str, in_data: impl IntoIterator<Item = KV>) -> Result<(), TableErr> {
        let index_file_name = format!("{}{}", file_name, INDEX_FILE_SUFFIX);
        let data_file_name = format!("{}{}", file_name, DATA_FILE_SUFFIX);
            
        let mut out_data: Vec<String> = Vec::new();
        let mut out_index: Vec<String> = Vec::new();

        let mut position = 0;
        for datum in in_data {
            out_index.push(format!("{}:{},{}", datum.key, position, datum.value.len()));

            position = position + datum.value.len();
            out_data.push(datum.value);
        }

        if let Err(data_write_error) = std::fs::write(data_file_name, out_data.join("")) {
            return Err(TableErr::IO(format!("Failed to write data file: {:?}", data_write_error)));
        }
        if let Err(index_write_error) = std::fs::write(index_file_name, out_index.join("\n")) {
            return Err(TableErr::IO(format!("Failed to write index file: {:?}", index_write_error)));
        }

        Ok(())
    }

    fn file_contains(file_name: &str, key: &str) -> Result<bool, TableErr> {
        match data_file_position(file_name, key)? {
            Some(_) => return Ok(true),
            None => return Ok(false),
        }
    }

    /// Reads the value for the given key.
    /// TODO: This currently reads the whole file into memory. That's obviously
    /// not what we want to be doing. We have the position of the value in the 
    /// file, so skip straight there and read it.
    fn read(file_name: &str, key: &str) -> Result<Option<String>, TableErr> {
        let Some(position) = data_file_position(file_name, key)? else {
            return Ok(None);
        };

        let data_file_name = format!("{}{}", file_name, DATA_FILE_SUFFIX);
        let data = std::fs::read_to_string(data_file_name);

        let start: usize = position.0.try_into().expect("Couldn't parse u32 into usize");
        let end: usize = (position.0 + position.1).try_into().expect("Couldn't parse u32 sum into usize");

        Ok(Some(data?[start..end].to_string()))
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
    }

    fn data_file_position(file_name: &str, key: &str) -> Result<Option<DataPosition>, TableErr> {
        let index_file_name = format!("{}{}", file_name, INDEX_FILE_SUFFIX);

        let index_file_reader = io::BufReader::new(File::open(index_file_name)?);

        for line in index_file_reader.lines() {
            let l = line?;

            if l.starts_with(key) {
                let parts: Vec<&str> = l.split(":").collect();
                if parts.len() < 2 {
                    return Err(TableErr::BadFile(format!("The key '{}' did not contain a corresponding position", l)));
                }

                let position_parts: Vec<&str> = parts[1].split(",").collect();

                if position_parts.len() < 2 {
                    return Err(TableErr::BadFile(format!("The key '{}' has an invalid position", l)));
                }

                return Ok(Some(DataPosition::from_strings(position_parts[0], position_parts[1])?));
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

        #[test]
        fn flushes() -> Result<(), TableErr> {
            let data = [
                KV { key: String::from("foo"), value: String::from("fooble") },
                KV { key: String::from("bar"), value: String::from("barble") },
                KV { key: String::from("baz"), value: String::from("bazzle") },
                KV { key: String::from("raz"), value: String::from("razzle") },
                KV { key: String::from("daz"), value: String::from("dazzle") },
            ];

            flush("test_files/test_flush_output", data)?;

            let data_file_contents = std::fs::read_to_string("test_files/test_flush_output.data")?;
            let index_file_contents = std::fs::read_to_string("test_files/test_flush_output.index")?;

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
        fn read_works() -> Result<(), TableErr> {
            let data = [
                KV { key: String::from("foo"), value: String::from("fooble") },
                KV { key: String::from("bar"), value: String::from("barble") },
                KV { key: String::from("baz"), value: String::from("bazzle") },
                KV { key: String::from("raz"), value: String::from("razzle") },
                KV { key: String::from("daz"), value: String::from("dazzle") },
            ];

            flush("test_files/test_read_output", data)?;

            assert_eq!("razzle", read("test_files/test_read_output", "raz")?.unwrap());

            Ok(())
        }
    }

}
