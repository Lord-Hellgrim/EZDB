/*
    EZQL spec
    Special reserved characters are
    ; 
    : 
    , 
    ..
    You cannot use these in the table header or in the names of primary keys

    Special reserved keywords are
    SELECT                  <-- \ 
    LEFT_JOIN               <--  \
    INNER_JOIN              <--    > Read queries
    RIGHT_JOIN              <--  /
    FULL_JOIN               <-- /
    
    DELETE                  <-- Write queries
    UPDATE                  <-- 
    
    THEN                    <-- Chain separate queries

    SUM                     <-- \
    AVERAGE                 <--  \
    MEDIAN                  <--    > Statistical queries
    MEAN                    <--  /
    COUNT(value)            <-- /
    
    __ROWID__               <-- Automatic primary key row header
    __RESULT__              <-- Name of transient table


    This is what an EZQL query looks like:
    
    
    White space next to separator characters ; : and , is ignored. The newlines are just for clarity.
    
    Supported filter tests are: equals, less, greater, starts, ends, contains
    Supported query types are: 
    Read queries
    SELECT, LEFT JOIN, INNER JOIN, RIGHT JOIN, FULL JOIN
    
    Write queries
    DELETE, UPDATE, INSERT, SAVE([new name])

    The result of a read query is a new reduced or expanded table according to the query.
    This new table is called __RESULT__. At the end of the query (including all chains),
    if there is a __RESULT__ table it will be returned to the querying client.
    The result of a write query is a change to the currently selected table according to the query
    You can chain read and write queries in any order. If you update a __RESULT__ table, there will
    be no change to the actual database until you SAVE the __RESULT__ with a name by using 
    the SAVE([new name]) command at the end of your update query. This will write the resulting 
    table to the database. The SAVE command does not drop the table and you can keep chaining 
    queries after it. Saving a __RESULT__ table creates a copy of it in the database. If you then
    chain a write query with the new table name, you will change the table in the database but if
    you use the __RESULT__ name, you only change the transient copy that will be returned to the
    querying client.
    The only difference between a chained query and a sequence of unchained queries is the __RESULT__ table.
    At the start of a query, the __RESULT__ table is empty. Essentially, "read" queries are write queries
    that only write to the __RESULT__ table. Each write query to a table other than __RESULT__ overwrites
    the current __RESULT__. So if you chain two SELECT queries to different named tables, you only
    get the result of the second query.

    example1:
    SELECT;                             <-- Type of query
    Products                            <-- Table name
    0113000..18572054;                  <-- List or range of keys to check. Use * to check all keys
    price: less, 500;                   <-- |\
    in_stock: greater, 100;             <-- | > Filters. Only keys from the list that meet these conditions will be selected
    location: equals, lag15;            <-- |/

    example1:
    DELETE;                             <-- Type of query
    Products                            <-- Table name
    0113000..18572054;                  <-- List or range of keys to check. Use * to check all keys
    price: less, 500;                   <-- |\
    in_stock: greater, 100;             <-- | > Filters. Only keys from the list that meet these conditions will be deleted
    location: equals, lag15;            <-- |/

    example3:
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


    Chaining queries:
    At the end of a query, the server internally has a ColumnTable that contains the elements 
    remaining after the initial query. If you use the THEN keyword at the end of a query you can then
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
    pub table: KeyString,
    pub query_type: QueryType,
    pub primary_keys: RangeOrListorAll,
    pub conditions: Vec<Condition>,
}

impl Query {
    pub fn new() -> Self {
        Query {
            table: KeyString::from("__RESULT__"),
            query_type: QueryType::SELECT,
            primary_keys: RangeOrListorAll::All,
            conditions: Vec::new(),
        }
    }
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum QueryType {
    SELECT,
    LEFT_JOIN,
    INNER_JOIN,
    RIGHT_JOIN,
    FULL_JOIN,
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
pub fn parse_query(query: &str) -> Result<Vec<Query>, ServerError> {

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