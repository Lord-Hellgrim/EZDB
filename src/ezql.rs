/*
    EZQL spec
    Special reserved characters are
    ; 
    : 
    , 
    ..
    You cannot use these in the table header or in the names of primary keys

    This is what an EZQL query looks like:
    [Type of query (ALL CAPS)];
    [Table name (or names if the query applies to multiple tables)];
    [list or range of primary keys (* for all items)];
    [attribute to filter by]: [test to apply], [what to test against];
    [another (or same) attribute]: [different test], [different bar];
    [if query is UPDATE then here you would put TO];
    [attribute to update]: [new value];

    White space next to separator characters ; : and , is ignored. The newlines are just for clarity.

    example1:
    SELECT;                             <-- Type of query
    Products                            <-- Table name
    0113000..18572054;                  <-- List or range of keys to check. Use * to check all keys
    price: less, 500;                   <-- |\
    in_stock: greater, 100;             <-- | > Filters. Only keys from the list that meet these conditions will be selected
    location: equals, lag15;            <-- |/

    example2:
    UPDATE;                             <-- Type of query
    Products;                           <-- Table name
    0113000, 0113034, 0113035, 0113500; <-- List or range of keys to check. Use * to check all keys
    price: less, 500;                   <-- |\
    price: greater, 200;                <-- | > Filters. Only keys from the list that meet these conditions will be selected
    location: equals, lag15;            <-- |/
    TO;                                 <-- Attributes after the TO statement are new values
    price: 400;                         <-- |\
    location: lag30;                    <-- | > All values that remain in the selection will be updated according to these specs.
    price: 500;                         <-- | > If an attribute is listed more than once, there is no guarantee which value will apply.
    in_stock: 5;                        <-- |/

    Supported filter tests are: equals, less, greater, starts, ends, contains
    Supported query types are: SELECT, UPDATE, LEFT JOIN, INNER JOIN, RIGHT JOIN, FULL JOIN

    Chaining queries:
    At the end of a query, the server internally has a ColumnTable that contains the elements 
    remaining after the initial query. If you use the CHAIN query at the end of a query you can then
    run a second query on the resulting table.

    Chain example1:
    LEFT JOIN;
    Products, warehouse1, warehouse2;
    *
    

*/


use smartstring::{LazyCompact, SmartString};

use crate::networking_utilities::ServerError;

pub type KeyString = SmartString<LazyCompact>;


/// A database query that has already been parsed from EZQL (see handlers.rs)
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Query {
    pub primary_keys: RangeOrListorAll,
    pub conditions: Vec<Condition>,
}

/// This enum represents the possible ways to list primary keys to test. 
/// See EZQL spec for details (handlers.rs).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RangeOrListorAll {
    Range([KeyString; 2]),
    List(Vec<KeyString>),
    All,
}

/// Represents the condition a item must pass to be included in the result
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Condition {
    pub attribute: KeyString,
    pub test: Test,
}

/// Represents the currenlty implemented tests
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Test {
    Equals(KeyString),
    Less(KeyString),
    Greater(KeyString),
    Starts(KeyString),
    Ends(KeyString),
    Contains(KeyString),
    //Closure,   could you imagine?
}

impl Test {
    pub fn new(input: &str, bar: &str) -> Self {
        match input {
            "equals" => Test::Equals(KeyString::from(bar)),
            "less" => Test::Less(KeyString::from(bar)),
            "greater" => Test::Greater(KeyString::from(bar)),
            "starts" => Test::Starts(KeyString::from(bar)),
            "ends" => Test::Ends(KeyString::from(bar)),
            "contains" => Test::Contains(KeyString::from(bar)),
            _ => todo!(),
        }
    }
}




/// Parses a EZQL query into a Query struct. Currently only select queries are implemented.
pub fn parse_query(query: &str) -> Result<Query, ServerError> {

    let mut output = Query {
        primary_keys: RangeOrListorAll::List(Vec::new()),
        conditions: Vec::new(),
    };

    let mut split_query = query.split(';');
    let items_to_test = match split_query.next() {
        Some(x) => x.trim(),
        None => return Err(ServerError::Query),
    };

    if items_to_test.trim() == "*" {
        output.primary_keys = RangeOrListorAll::All;
    } else {

        match items_to_test.find("..") {
            Some(_) => {
                let mut temp_split = items_to_test.split("..");
                let start = match temp_split.next() {
                    Some(x) => x.trim(),
                    None => return Err(ServerError::Query),
                };
                let stop = match temp_split.next() {
                    Some(x) => x.trim(),
                    None => return Err(ServerError::Query),
                };
                output.primary_keys = RangeOrListorAll::Range([KeyString::from(start), KeyString::from(stop)]);
            },
            None => {
                let list: Vec<KeyString> = items_to_test.split(',').map(|x| KeyString::from(x.trim())).collect();
                output.primary_keys = RangeOrListorAll::List(list);
            },
        };
    }

    println!("PK's: {}", items_to_test);

    let conditions: Vec<&str> = split_query.map(|x| x.trim()).collect();

    let mut tests = Vec::with_capacity(conditions.len());
    for condition in &conditions {
        let mut split = condition.split(':');
        let attribute = match split.next() {
            Some(x) => x.trim(),
            None => return Err(ServerError::Query),
        };

        let test_bar = match split.next() {
            Some(x) => x.trim(),
            None => return Err(ServerError::Query),
        };

        let mut test_bar_split = test_bar.split(',');

        let test = match test_bar_split.next() {
            Some(x) => x.trim(),
            None => return Err(ServerError::Query),
        };

        println!("test: {}", test);

        let bar = match test_bar_split.next() {
            Some(x) => x.trim(),
            None => return Err(ServerError::Query),
        };

        let t = Condition {
            attribute: KeyString::from(attribute),
            test: Test::new(test, bar),
        };

        tests.push(t);
    }

    output.conditions = tests;

    Ok(output)
}



#[cfg(test)]

mod tests {

    use super::*;

    #[test]
    fn test_parse_query() {
        let query = "0113000, 0113034, 0113035, 0113500;
        price: less, 500;
        price: greater, 200;
        location: equals, lag15";
        let query = parse_query(query).unwrap();

        let test_query = Query {
            primary_keys: RangeOrListorAll::List(vec![KeyString::from("0113000"), KeyString::from("0113034"), KeyString::from("0113035"), KeyString::from("0113500")]),
            conditions: vec![
                Condition {
                    attribute: KeyString::from("price"),
                    test: Test::Less(KeyString::from("500")),
                },
                Condition {
                    attribute: KeyString::from("price"),
                    test: Test::Greater(KeyString::from("200")),
                },
                Condition {
                    attribute: KeyString::from("location"),
                    test: Test::Equals(KeyString::from("lag15")),
                },
            ]
        };

        assert_eq!(query, test_query);
        dbg!(query);
    }

}