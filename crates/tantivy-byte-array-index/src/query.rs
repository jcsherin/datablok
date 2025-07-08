pub mod boolean_query {
    use crate::error::Error::FieldNotFound;
    use crate::error::Result;
    use tantivy::Term;
    use tantivy::query::{BooleanQuery, Occur, Query, QueryClone, TermQuery};
    use tantivy::schema::{IndexRecordOption, Schema};

    /// Make a boolean query equivalent to: title:+diary title:-girl
    pub fn title_contains_diary_and_not_girl(schema: &Schema) -> Result<BooleanQuery> {
        let title_field = schema
            .get_field("title")
            .map_err(|_| FieldNotFound(String::from("title")))?;

        // A term query matches all the documents containing a specific term.
        // The `Query` trait defines a set of documents and a way to score those documents.
        let girl_term_query: Box<dyn Query> = Box::new(TermQuery::new(
            Term::from_field_text(title_field, "girl"),
            IndexRecordOption::Basic, // records only the `DocId`s
        ));
        let diary_term_query: Box<dyn Query> = Box::new(TermQuery::new(
            Term::from_field_text(title_field, "diary"),
            IndexRecordOption::Basic,
        ));

        let subqueries = vec![
            (Occur::Must, diary_term_query.box_clone()),
            (Occur::MustNot, girl_term_query.box_clone()),
        ];

        Ok(BooleanQuery::new(subqueries))
    }
}
