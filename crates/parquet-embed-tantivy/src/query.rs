pub mod boolean_query {
    use crate::error::Error::FieldNotFound;
    use crate::error::Result;
    use tantivy::query::{BooleanQuery, Occur, PhraseQuery, Query, TermQuery};
    use tantivy::schema::{Field, IndexRecordOption, Schema};
    use tantivy::Term;

    fn girl_term_query(title: Field) -> Box<dyn Query> {
        Box::new(TermQuery::new(
            Term::from_field_text(title, "girl"),
            IndexRecordOption::Basic,
        ))
    }

    fn diary_term_query(title: Field) -> Box<dyn Query> {
        Box::new(TermQuery::new(
            Term::from_field_text(title, "diary"),
            IndexRecordOption::Basic,
        ))
    }

    fn cow_term_query(title: Field) -> Box<dyn Query> {
        Box::new(TermQuery::new(
            Term::from_field_text(title, "cow"),
            IndexRecordOption::Basic,
        ))
    }

    #[allow(dead_code)]
    fn body_term_query(body: Field) -> Box<dyn Query> {
        Box::new(TermQuery::new(
            Term::from_field_text(body, "found"),
            IndexRecordOption::Basic,
        ))
    }

    /// Make a boolean query equivalent to: title:+diary title:-girl
    pub fn title_contains_diary_and_not_girl(schema: &Schema) -> Result<BooleanQuery> {
        let title_field = schema
            .get_field("title")
            .map_err(|_| FieldNotFound(String::from("title")))?;

        let subqueries = vec![
            (Occur::Must, diary_term_query(title_field)),
            (Occur::MustNot, girl_term_query(title_field)),
        ];

        Ok(BooleanQuery::new(subqueries))
    }

    /// title:diary OR title:cow
    pub fn title_contains_diary_or_cow(schema: &Schema) -> Result<BooleanQuery> {
        let title = schema
            .get_field("title")
            .map_err(|_| FieldNotFound(String::from("title")))?;

        let subqueries = vec![
            (Occur::Should, diary_term_query(title)),
            (Occur::Should, cow_term_query(title)),
        ];

        Ok(BooleanQuery::new(subqueries))
    }

    /// title:"dairy cow"
    pub fn title_contains_phrase_diary_cow(title: Field) -> Box<dyn Query> {
        Box::new(PhraseQuery::new(vec![
            Term::from_field_text(title, "dairy"),
            Term::from_field_text(title, "cow"),
        ]))
    }

    /// title:diary OR title:"dairy cow"
    pub fn combine_term_and_phrase_query(schema: &Schema) -> Result<BooleanQuery> {
        let title = schema
            .get_field("title")
            .map_err(|_| FieldNotFound(String::from("title")))?;

        Ok(BooleanQuery::new(vec![
            (Occur::Should, diary_term_query(title)),
            (Occur::Should, title_contains_phrase_diary_cow(title)),
        ]))
    }
}
