#[derive(Debug)]
pub struct KV {
    pub key: String,
    pub value: String,
}

impl Clone for KV {
    fn clone(&self) -> Self {
        KV { 
            key: self.key.to_string(),
            value: self.value.to_string(),
        }
    }
}

