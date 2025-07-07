use crate::doc::{Doc, DocSchema};
use log::info;
use tantivy::{Index, IndexWriter, TantivyDocument};

mod doc {
    use tantivy::schema::{Schema, SchemaBuilder, TEXT};

    pub struct Doc {
        title: String,
        body: Option<String>,
    }

    impl Doc {
        pub fn new(title: String, body: Option<String>) -> Self {
            Self { title, body }
        }

        pub fn title(&self) -> &str {
            &self.title
        }

        pub fn body(&self) -> Option<&str> {
            self.body.as_deref()
        }
    }

    pub struct DocSchema(Schema);
    impl Default for DocSchema {
        fn default() -> Self {
            let mut schema_builder = SchemaBuilder::new();
            schema_builder.add_text_field("title", TEXT);
            schema_builder.add_text_field("body", TEXT);

            Self(schema_builder.build())
        }
    }

    impl DocSchema {
        pub fn into_schema(self) -> Schema {
            self.0
        }
    }
}

fn docs() -> Vec<Doc> {
    vec![
        ("The Name of the Wind".to_string(), None),
        ("The Diary of Muadib".to_string(), None),
        ("A Dairy Cow".to_string(), Some("hidden".to_string())),
        ("A Dairy Cow".to_string(), Some("found".to_string())),
        ("The Diary of a Young Girl".to_string(), None),
    ]
    .into_iter()
    .map(|(title, body)| Doc::new(title, body))
    .collect()
}

fn main() -> tantivy::Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let index = Index::create_in_ram(DocSchema::default().into_schema());

    {
        let mut index_writer: IndexWriter = index.writer(50_000_000)?;

        let schema = DocSchema::default().into_schema();
        let title_field = schema.get_field("title")?;
        let body_field = schema.get_field("body")?;

        for doc in docs() {
            let mut tantivy_doc = TantivyDocument::default();
            tantivy_doc.add_text(title_field, doc.title());
            if let Some(body) = doc.body() {
                tantivy_doc.add_text(body_field, body);
            }

            index_writer.add_document(tantivy_doc)?;
        }
        index_writer.commit()?;
    }

    info!("{:#?}", index.directory());

    Ok(())
}
