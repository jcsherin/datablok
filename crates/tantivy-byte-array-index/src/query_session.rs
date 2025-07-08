use crate::indexer::ImmutableIndex;
use tantivy::collector::Collector;
use tantivy::query::Query;
use tantivy::schema::Schema;
use tantivy::{IndexReader, Searcher};

pub struct QuerySession<'a> {
    index: &'a ImmutableIndex,
    _reader: IndexReader,
    searcher: Searcher,
}

impl<'a> QuerySession<'a> {
    pub fn new(index: &'a ImmutableIndex) -> tantivy::Result<Self> {
        let reader = index.reader()?;
        let searcher = reader.searcher();

        Ok(QuerySession {
            index,
            _reader: reader,
            searcher,
        })
    }

    pub fn schema(&self) -> &Schema {
        self.index.schema()
    }

    pub fn searcher(&self) -> &Searcher {
        &self.searcher
    }

    pub fn search<C: Collector>(
        &self,
        query: &dyn Query,
        collector: &C,
    ) -> tantivy::Result<C::Fruit> {
        self.searcher.search(query, collector)
    }
}
