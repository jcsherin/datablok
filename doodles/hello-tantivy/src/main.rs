use log::{debug, info};
use std::fs;
use tantivy::schema::*;
use tantivy::{Index, IndexWriter, doc};
use tempfile::TempDir;

// https://github.com/quickwit-oss/tantivy/blob/main/examples/basic_search.rs
fn main() -> tantivy::Result<()> {
    env_logger::init();

    let index_path = TempDir::new()?;
    info!("Index created at: {}", index_path.path().display());

    let mut schema_builder = Schema::builder();

    schema_builder.add_text_field("title", TEXT | STORED);
    schema_builder.add_text_field("body", TEXT);

    let schema = schema_builder.build();
    let schema_json = serde_json::to_string_pretty(&schema)?;
    debug!("Schema: {}", schema_json);

    let index = Index::create_in_dir(&index_path, schema.clone())?;

    // Budget for indexing in bytes -> `50MB`.
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // Manually constructing the document by setting the fields one by one in the document
    let title = schema.get_field("title")?;
    let body = schema.get_field("body")?;

    let mut old_man_doc = TantivyDocument::default();
    old_man_doc.add_text(title, "The Old Man and the Sea");
    old_man_doc.add_text(
        body,
        "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone \
         eighty-four days now without taking a fish.",
    );

    index_writer.add_document(old_man_doc)?;

    // Uses the macro for convenience
    index_writer.add_document(doc!(
    title => "Of Mice and Men",
    body => "A few miles south of Soledad, the Salinas River drops in close to the hillside \
            bank and runs deep and green. The water is warm too, for it has slipped twinkling \
            over the yellow sands in the sunlight before reaching the narrow pool. On one \
            side of the river the golden foothill slopes curve up to the strong and rocky \
            Gabilan Mountains, but on the valley side the water is lined with trees—willows \
            fresh and green with every spring, carrying in their lower leaf junctures the \
            debris of the winter’s flooding; and sycamores with mottled, white, recumbent \
            limbs and branches that arch over the pool"
    ))?;

    // Multivalued field just need to be repeated.
    index_writer.add_document(doc!(
    title => "Frankenstein",
    title => "The Modern Prometheus",
    body => "You will rejoice to hear that no disaster has accompanied the commencement of an \
             enterprise which you have regarded with such evil forebodings.  I arrived here \
             yesterday, and my first task is to assure my dear sister of my welfare and \
             increasing confidence in the success of my undertaking."
    ))?;

    // The call is blocking
    index_writer.commit()?;
    info!("Commit: 3 documents written to index");

    fs::read_dir(index_path.path())?
        .filter_map(|entry_result| {
            let entry = entry_result.ok()?;
            let path = entry.path();
            let filename_ref = path.file_name()?;
            Some(filename_ref.to_os_string())
        })
        .for_each(|file| info!("Found file: {:?}", file));

    Ok(())
}
