use crate::error::Result;
use crate::index::ImmutableIndex;
use schema::Schema;
use tantivy::collector::Collector;
use tantivy::query::Query;
use tantivy::{IndexReader, Searcher, schema};

pub struct QuerySession<'a> {
    index: &'a ImmutableIndex,
    _reader: IndexReader,
    searcher: Searcher,
}

impl<'a> QuerySession<'a> {
    pub fn new(index: &'a ImmutableIndex) -> Result<Self> {
        let reader = index.reader()?;
        let searcher = reader.searcher();

        Ok(QuerySession {
            index,
            _reader: reader,
            searcher,
        })
    }

    pub fn searcher(&self) -> &Searcher {
        &self.searcher
    }

    pub fn schema(&self) -> Schema {
        self.index.schema()
    }

    pub fn search<C: Collector>(&self, query: &dyn Query, collector: &C) -> Result<C::Fruit> {
        Ok(self.searcher.search(query, collector)?)
    }
}
