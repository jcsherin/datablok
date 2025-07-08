pub struct Config {
    pub index_writer_memory_budget_in_bytes: usize,
    pub id_field_name: String,
    pub title_field_name: String,
    pub body_field_name: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            index_writer_memory_budget_in_bytes: 50_000_000,
            id_field_name: "id".to_string(),
            title_field_name: "title".to_string(),
            body_field_name: "body".to_string(),
        }
    }
}
