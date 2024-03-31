/*
    EZQL spec
    Special reserved characters are
    ; 
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
    [Query Type];
    [Table name];
    [Primary keys to test];
    [Conditions to apply];
    [New values if any];
    [Chain if any];
    ... and next query can be chained

    Example:
    SELECT;                                     <-- Type of query. See all query types at the top of this file
    Products;                                   <-- Name of table.
    *;                                          <-- Primary keys. Can either be full list separated by commas, a range from..to separated by '..'
    (price greater-than 500)                    <-- \
    AND                                         <--  \
    (price less-than 1000)                      <--    > List of conditions to filter by. Conditions should be enclosed in parentheses and 
    OR                                          <--    > separated by whitespace (attribute test bar). Each condition can then be combined
    ("in stock" greater-than 50)                <--    > with another with AND, OR, NOT. Precedence is NOT > AND > OR
    AND                                         <--  /
    (name contains "steel screw");              <-- /  (note the trailing semicolon here. The list of conditions must end with ';' if you will chain another query)
    THEN;
    UPDATE;
    Products;
    *;
    (name contains "steel screw")
    OR
    (name contains "wood screw")
    TO;
    price += 400;
    "in stock" *= 1.15;
    name append " *Updated";

    Whitespace next to separator characters ; : and , is ignored. The newlines are just for clarity.
    
    Supported filter tests are: 
    equals, less-than, greater-than, starts-with, ends-with, contains
    
    Supported query types are: 
    Read queries
    SELECT, LEFT JOIN, INNER JOIN, RIGHT JOIN, FULL JOIN
    
    Write queries
    DELETE, UPDATE, INSERT, SAVE([new name])

    Supported update operations are:
    INTS:   '=', '+=', '*='  (Note that -= and /= are not supported but are subclasses of += and *=)
    FLOATS: '=', '+=', '*='  (Note that -= and /= are not supported but are subclasses of += and *=)
    TEXT:   append, assign, prepend, 

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

    example4:
    INSERT;                             <-- Type of query
    Products;                           <-- Table name (Here all the table column names are "id", "name", "price")
    name, price, id;                    <-- Identifies which item in the following list of rows maps to which column in the table. Order is irrelevant.
    (hammer, 500, 60401011),            <-- |\
    (screwdriver, 100, 60401010),       <-- | > New values. If a value with the same primary key as a listed value exists in the table, it will not be updated.
    (chewing gum, 50, 1323),            <-- |/ 



    Chaining queries:
    At the end of a query, the server internally has a ColumnTable that contains the elements 
    remaining after the initial query. If you use the THEN keyword at the end of a query you can then
    run a second query on the resulting table.

    Chain example1:
    LEFT JOIN;
    Products, warehouse1, warehouse2;
    *
    
    NOT > AND > OR

    SELECT;
    Products;
    *;
    (price greater-than 500)
    AND
    (price less-than 1000)
    OR
    ("in stock" greater-than 50)
    AND
    (name contains "steel screw")
    


*/

use std::{fmt::Display, sync::Arc};

use crate::{db_structure::{remove_indices, DbColumn, EZTable, KeyString}, networking_utilities::ServerError, server_networking::Database};


#[derive(Debug, PartialEq)]
pub enum QueryError {
    InvalidQueryType,
    InvalidConditionFormat,
    InvalidTest,
    InvalidTO,
    InvalidUpdate,
    TableNameTooLong,
    Unknown,
}


/// A database query that has already been parsed from EZQL (see handlers.rs)
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Query {
    pub table: KeyString,
    pub query_type: QueryType,
    pub primary_keys: RangeOrListorAll,
    pub conditions: Vec<OpOrCond>,
    pub updates: Vec<Update>,
}

impl Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut printer = String::new();
        printer.push_str(&self.query_type.to_string());
        printer.push_str("\n");
        printer.push_str(self.table.as_str());
        printer.push_str("\n");
        match &self.primary_keys {
            RangeOrListorAll::Range(start, stop) => printer.push_str(&format!("{}..{}", start.as_str(), stop.as_str())),
            RangeOrListorAll::List(list) => {
                for item in list {
                    printer.push_str(item.as_str());
                    printer.push_str(",");
                }
            },
            RangeOrListorAll::All => printer.push_str("*"),
        }
        printer.push_str("\n");
        for condition in &self.conditions {
            printer.push_str(&condition.to_string());
            printer.push_str("\n");
        }
        for update in &self.updates {
            printer.push_str(&update.to_string());
            printer.push_str("\n");
        }
        printer.pop();



        write!(f, "{}", printer)
    }

}

impl Query {
    pub fn new() -> Self {
        Query {
            table: KeyString::from("__RESULT__"),
            query_type: QueryType::SELECT,
            primary_keys: RangeOrListorAll::All,
            conditions: Vec::new(),
            updates: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Update {
    attribute: KeyString,
    operator: UpdateOp,
    value: KeyString,
}

impl Display for Update {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let op = match self.operator {
            UpdateOp::Assign => "=",
            UpdateOp::PlusEquals => "+=",
            UpdateOp::TimesEquals => "*=",
            UpdateOp::Append => "append",
            UpdateOp::Prepend => "prepend",
        };
        write!(f, "({} {} {})", self.attribute.as_str(), op, self.value.as_str())
    }
}

impl Update {

    pub fn blank() -> Self{
        Update {
            attribute: KeyString::new(),
            operator: UpdateOp::Assign,
            value: KeyString::new(),
        }
    }

    pub fn from_str(s: &str) -> Result<Self, QueryError> {
        let output: Update;
        let mut t = s.split_whitespace();
        if s.split_whitespace().count() < 3 {
            return Err(QueryError::InvalidUpdate)
        }
        if s.split_whitespace().count() == 3 {
            output = Update {
                attribute: KeyString::from(t.next().unwrap()),
                operator: UpdateOp::from_str(t.next().unwrap())?,
                value: KeyString::from(t.next().unwrap()),
            };
        } else {
            let mut acc = Vec::new();
            let mut buf = String::new();
            let mut inside = false;
            for c in s.chars() {
                if acc.len() > 3 {break;}
                // println!("buf: {}", buf);
                if c.is_whitespace() {
                    if inside {
                        buf.push(c);
                        continue;
                    } else {
                        acc.push(buf.clone());
                        buf.clear();
                        // println!("acc: {:?}", acc);
                        continue;
                    }
                } else if c == '"' {
                    inside = inside ^ true;
                    continue;
                } else {
                    buf.push(c);
                }
            }
            acc.push(buf);

            if acc.len() == 3 {
                output = Update {
                    attribute: KeyString::from(acc[0].as_str()),
                    operator: UpdateOp::from_str(acc[1].as_str())?,
                    value: KeyString::from(acc[2].as_str()),
                };
            } else {
                return Err(QueryError::InvalidUpdate)
            }
        }

        Ok(output)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum UpdateOp {
    Assign,
    PlusEquals,
    TimesEquals,
    Append,
    Prepend,
}

impl UpdateOp {
    fn from_str(s: &str) -> Result<Self, QueryError> {
        match s {
            "=" => Ok(UpdateOp::Assign),
            "+=" => Ok(UpdateOp::PlusEquals),
            "*=" => Ok(UpdateOp::TimesEquals),
            "append" => Ok(UpdateOp::Append),
            "assign" => Ok(UpdateOp::Assign),
            "prepend" => Ok(UpdateOp::Prepend),
            _ => return Err(QueryError::InvalidUpdate),
        }
    }
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[allow(non_camel_case_types)]
pub enum QueryType {
    SELECT,
    LEFT_JOIN,
    INNER_JOIN,
    RIGHT_JOIN,
    FULL_JOIN,
    UPDATE,
    INSERT,
    DELETE,
}

impl Display for QueryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryType::SELECT => write!(f, "SELECT", ),
            QueryType::LEFT_JOIN => write!(f, "LEFT_JOIN", ),
            QueryType::INNER_JOIN => write!(f, "INNER_JOIN", ),
            QueryType::RIGHT_JOIN => write!(f, "RIGHT_JOIN", ),
            QueryType::FULL_JOIN => write!(f, "FULL_JOIN", ),
            QueryType::UPDATE => write!(f, "UPDATE", ),
            QueryType::INSERT => write!(f, "INSERT", ),
            QueryType::DELETE => write!(f, "DELETE", ),
        }
    }
}

/// This enum represents the possible ways to list primary keys to test. 
/// See EZQL spec for details (handlers.rs).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RangeOrListorAll {
    Range(KeyString, KeyString),
    List(Vec<KeyString>),
    All,
}

/// Represents the condition a item must pass to be included in the result
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Condition {
    pub attribute: KeyString,
    pub test: Test,
}

impl Condition {

    pub fn new(attribute: &str, test: &str, bar: &str) -> Result<Self, QueryError> {
        let test = match test {
            "equals" => Test::Equals(KeyString::from(bar)),
            "less" => Test::Less(KeyString::from(bar)),
            "greater" => Test::Greater(KeyString::from(bar)),
            "starts" => Test::Starts(KeyString::from(bar)),
            "ends" => Test::Ends(KeyString::from(bar)),
            "contains" => Test::Contains(KeyString::from(bar)),
            _ => return Err(QueryError::InvalidTest)
        };

        Ok(Condition {
            attribute: KeyString::from(attribute),
            test,
        })
    }

    fn from_str(s: &str) -> Result<Self, QueryError> {
        let output: Condition;
        let mut t = s.split_whitespace();
        if s.split_whitespace().count() < 3 {
            return Err(QueryError::InvalidConditionFormat)
        }
        if s.split_whitespace().count() == 3 {
            output = Condition {
                attribute: KeyString::from(t.next().unwrap()),
                test: Test::new(t.next().unwrap(), t.next().unwrap()),
            };
        } else {
            let mut acc = Vec::new();
            let mut buf = String::new();
            let mut inside = false;
            for c in s.chars() {
                // println!("buf: {}", buf);
                if c.is_whitespace() {
                    if inside {
                        buf.push(c);
                        continue;
                    } else {
                        acc.push(buf.clone());
                        buf.clear();
                        // println!("acc: {:?}", acc);
                        continue;
                    }
                } else if c == '"' {
                    inside = inside ^ true;
                    continue;
                } else {
                    buf.push(c);
                }
            }
            acc.push(buf);

            if acc.len() == 3 {
                output = Condition::new(&acc[0], &acc[1], &acc[2])?;
            } else {
                return Err(QueryError::InvalidConditionFormat)
            }
        }

        Ok(output)
    }

    pub fn blank() -> Self {
        Condition {
            attribute: KeyString::from(""),
            test: Test::Equals(KeyString::from("")),
        }
    }
}



#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Operator {
    AND,
    OR,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum OpOrCond {
    Cond(Condition),
    Op(Operator),
}

impl Display for OpOrCond {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpOrCond::Cond(cond) => write!(f, "({}: {})", cond.attribute, cond.test.to_string()),
            OpOrCond::Op(op) => match op {
                Operator::AND => write!(f, "AND"),
                Operator::OR => write!(f, "OR"),
            },
        }
    }
}


/// Represents the currenlty implemented tests
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Test {
    Equals(KeyString),
    NotEquals(KeyString),
    Less(KeyString),
    Greater(KeyString),
    Starts(KeyString),
    Ends(KeyString),
    Contains(KeyString),
    //Closure,   could you imagine?
}

impl Display for Test {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Test::Equals(value) => write!(f, "equals {}", value),
            Test::NotEquals(value) => write!(f, "not_equals {}", value),
            Test::Less(value) => write!(f, "less-than {}", value),
            Test::Greater(value) => write!(f, "greater-than {}", value),
            Test::Starts(value) => write!(f, "starts-with {}", value),
            Test::Ends(value) => write!(f, "ends-with {}", value),
            Test::Contains(value) => write!(f, "contains {}", value),
        }
    }
}

impl Test {
    pub fn new(input: &str, bar: &str) -> Self {
        match input.to_lowercase().as_str() {
            "equals" => Test::Equals(KeyString::from(bar)),
            "not_equals" => Test::NotEquals(KeyString::from(bar)),
            "less" => Test::Less(KeyString::from(bar)),
            "greater" => Test::Greater(KeyString::from(bar)),
            "starts" => Test::Starts(KeyString::from(bar)),
            "ends" => Test::Ends(KeyString::from(bar)),
            "contains" => Test::Contains(KeyString::from(bar)),
            _ => todo!(),
        }
    }
}

pub enum ConditionBranch<'a> {
    Branch(Vec<&'a ConditionBranch<'a>>),
    Leaf(Condition),
}

enum Expect {
    QueryType,
    TableName,
    PrimaryKeys,
    Conditions,
    Updates,
    Inserts,
}

#[allow(non_snake_case)]
pub fn parse_EZQL(query_string: &str) -> Result<Vec<Query>, QueryError> {

    let mut queries = Vec::new();
    
    let mut expect = Expect::QueryType;
    let mut query_buf = Query::new();
    for token in query_string.split(';') {
        // println!("token: {}", token);
        match expect {
            Expect::QueryType => {
                match token.trim() {
                    "SELECT" => {
                        query_buf.query_type = QueryType::SELECT;
                        expect = Expect::TableName;
                    },
                    "LEFT_JOIN" => {
                        query_buf.query_type = QueryType::LEFT_JOIN;
                        expect = Expect::TableName;
                    },
                    "INNER_JOIN" => {
                        query_buf.query_type = QueryType::INNER_JOIN;
                        expect = Expect::TableName;
                    },
                    "RIGHT_JOIN" => {
                        query_buf.query_type = QueryType::RIGHT_JOIN;
                        expect = Expect::TableName;
                    },
                    "FULL_JOIN" => {
                        query_buf.query_type = QueryType::FULL_JOIN;
                        expect = Expect::TableName;
                    },
                    "DELETE" => {
                        query_buf.query_type = QueryType::DELETE;
                        expect = Expect::TableName;
                    },
                    "UPDATE" => {
                        query_buf.query_type = QueryType::UPDATE;
                        expect = Expect::TableName;
                    },
                    "INSERT" => {
                        query_buf.query_type = QueryType::INSERT;
                        expect = Expect::TableName;
                    }

                    _ => return Err(QueryError::InvalidQueryType),
                }
            },
            Expect::TableName => {
                let x = token.trim();
                if x.len() > 64 {
                    return Err(QueryError::TableNameTooLong);
                } else if query_buf.query_type == QueryType::INSERT {
                    query_buf.table = KeyString::from(x);
                    expect = Expect::Inserts;

                } else {
                    query_buf.table = KeyString::from(x);
                    expect = Expect::PrimaryKeys;

                }
            },
            Expect::PrimaryKeys => {
                let tok = token.trim();
                if tok.trim().split("..").count() == 2 {
                let mut ranger = tok.split("..");
                query_buf.primary_keys = RangeOrListorAll::Range(
                    KeyString::from(ranger.next().unwrap().trim()), 
                    KeyString::from(ranger.next().unwrap().trim())
                );
                    expect = Expect::Conditions;
                } else if tok == "*" {
                    query_buf.primary_keys = RangeOrListorAll::All;
                    expect = Expect::Conditions;
                } else {
                    query_buf.primary_keys = RangeOrListorAll::List(tok.split(',').map(|n| KeyString::from(n.trim())).collect());
                    expect = Expect::Conditions;
                }

            },
            Expect::Conditions => {
                let other = token.trim();
                let mut blocks = Vec::new();
                let mut pos = 0;
                while pos < other.len() {
                    // println!("pos: {}", pos);
                    // println!("blocks: {:?}", blocks);
                    if other.as_bytes()[pos] == b'(' {
                        let block = match parse_contained_token(&other[pos..], '(', ')') {
                            Some(z) => z,
                            None => return Err(QueryError::InvalidConditionFormat),
                        }; 
                        blocks.push(block);
                        pos += block.len() + 2;
                        continue;
                    } else if other[pos..].starts_with("AND") || other[pos..].starts_with("OR") ||other[pos..].starts_with("NOT") {
                        blocks.push(other[pos..pos+3].trim());
                    } else if other[pos..].starts_with("THEN") {
                        queries.push(query_buf.clone());
                        query_buf = Query::new();
                        expect = Expect::QueryType;
                        break;
                    } else if other[pos..].starts_with("TO") {
                        if query_buf.query_type != QueryType::UPDATE {
                            return Err(QueryError::InvalidTO)
                        } else {
                            expect = Expect::Updates;
                        }
                        break;
                    }
                    pos += 1;
                }

                let mut op_or_cond_queue = Vec::new();
                for block in blocks {
                    match block {
                        "AND" => op_or_cond_queue.push(OpOrCond::Op(Operator::AND)),
                        "OR" => op_or_cond_queue.push(OpOrCond::Op(Operator::OR)),
                        other => {
                            op_or_cond_queue.push(OpOrCond::Cond(Condition::from_str(other)?));
                        }
                    }
                }
                query_buf.conditions = op_or_cond_queue;
            },

            Expect::Updates => {

                let other = token.trim();
                let mut blocks = Vec::new();
                let mut pos = 0;
                while pos < other.len() {
                    // println!("pos: {}", pos);
                    // println!("blocks: {:?}", blocks);
                    if other.as_bytes()[pos] == b'(' {
                        let block = match parse_contained_token(&other[pos..], '(', ')') {
                            Some(z) => z,
                            None => return Err(QueryError::InvalidUpdate),
                        }; 
                        pos += block.len() + 2;
                        blocks.push(block);
                        continue;
                    }
                    pos += 1;
                }

                let mut update_queue = Vec::new();
                for block in blocks {
                    update_queue.push(Update::from_str(block)?);
                }
                query_buf.updates = update_queue;
            },

            Expect::Inserts => {
                let tok = token.trim();

            }
        };
    }

    queries.push(query_buf);

    Ok(queries)
}

pub fn is_even(x: usize) -> bool {
    0 == (x & 1)
}


pub fn parse_contained_token<'a>(s: &'a str, container_open: char, container_close: char) -> Option<&'a str> {
    let mut start = std::usize::MAX;
    let mut stop = 0;
    let mut inside = false;
    for (index, c) in s.chars().enumerate() {
        // println!("start: {}\tstop: {}\tindex: {}", start, stop, index);
        stop += 1;
        match c {
            x if x == container_open => {
                match inside {
                    true => {
                        if container_open == container_close {
                            stop = index;
                            break;
                        } else {
                            continue;
                        }
                    },
                    false => {
                        inside = true;
                        start = index + 1;
                    }
                };
            },
            x if x == container_close => {
                match inside {
                    true => {
                        stop = index;
                        break;
                    },
                    false => {
                        continue;
                    }
                };
            },
            _ => continue,
        };
    }

    if stop < start {
        return None
    }

    Some(&s[start..stop])
}

#[allow(non_snake_case)]
pub fn execute_single_EZQL_query(query: Query, database: Arc<Database>) -> Result<EZTable, ServerError> {

    match query.query_type {
        QueryType::DELETE => {
            execute_delete_query(query, database)
        },
        QueryType::SELECT => {
            execute_select_query(query, database)
        },
        QueryType::LEFT_JOIN => {
            execute_left_join_query(query, database)
        },
        QueryType::INNER_JOIN => {
            execute_inner_join_query(query, database)
        },
        QueryType::RIGHT_JOIN => {
            execute_right_join_query(query, database)
        },
        QueryType::FULL_JOIN => {
            execute_full_join_query(query, database)
        },
        QueryType::UPDATE => {
            execute_update_query(query, database)
        },
        QueryType::INSERT => {
            execute_insert_query(query, database)
        },
    }
}

fn execute_delete_query(query: Query, database: Arc<Database>) -> Result<EZTable, ServerError> {
    let tables = database.buffer_pool.tables.read().unwrap();
    let mut table = tables.get(&query.table).unwrap().write().unwrap();
    let keepers = filter_keepers(&query, &table)?;
    table.delete_by_indexes(&keepers);

    Ok(
        table.clone()
    )
}

fn execute_left_join_query(query: Query, database: Arc<Database>) -> Result<EZTable, ServerError> {
    let tables = database.buffer_pool.tables.read().unwrap();
    let table = tables.get(&query.table).unwrap().read().unwrap();
    let keepers = filter_keepers(&query, &table)?;
    return Err(ServerError::Unimplemented("Joins are not yet implemented".to_owned()));
    
}

fn execute_inner_join_query(query: Query, database: Arc<Database>) -> Result<EZTable, ServerError> {
    let tables = database.buffer_pool.tables.read().unwrap();
    let table = tables.get(&query.table).unwrap().read().unwrap();
    let keepers = filter_keepers(&query, &table)?;

    return Err(ServerError::Unimplemented("Joins are not yet implemented".to_owned()));
}

fn execute_right_join_query(query: Query, database: Arc<Database>) -> Result<EZTable, ServerError> {
    let tables = database.buffer_pool.tables.read().unwrap();
    let table = tables.get(&query.table).unwrap().read().unwrap();
    let keepers = filter_keepers(&query, &table)?;

    return Err(ServerError::Unimplemented("Joins are not yet implemented".to_owned()));
}

fn execute_full_join_query(query: Query, database: Arc<Database>) -> Result<EZTable, ServerError> {
    let tables = database.buffer_pool.tables.read().unwrap();
    let table = tables.get(&query.table).unwrap().read().unwrap();
    let keepers = filter_keepers(&query, &table)?;

    return Err(ServerError::Unimplemented("Joins are not yet implemented".to_owned()));
}

// pub struct Update {
//     attribute: KeyString,
//     Operator: UpdateOp,
//     Value: KeyString,
// }

fn execute_update_query(query: Query, database: Arc<Database>) -> Result<EZTable, ServerError> {
    let tables = database.buffer_pool.tables.read().unwrap();
    let mut table = tables.get(&query.table).unwrap().write().unwrap();
    let keepers = filter_keepers(&query, &table)?;

    for update in &query.updates{
        for keeper in &keepers {
            if !table.columns.contains_key(&update.attribute) {
                return Err(ServerError::Query)
            }
            match update.operator {
                UpdateOp::Assign => {
                    match table.columns.get_mut(&update.attribute).unwrap() {
                        DbColumn::Ints(ref mut column) => column[*keeper] = update.value.to_i32(),
                        DbColumn::Floats(ref mut column) => column[*keeper] = update.value.to_f32(),
                        DbColumn::Texts(ref mut column) => column[*keeper] = update.value.clone(),
                    }
                },
                UpdateOp::PlusEquals => {
                    match table.columns.get_mut(&update.attribute).unwrap() {
                        DbColumn::Ints(ref mut column) => column[*keeper] += update.value.to_i32(),
                        DbColumn::Floats(ref mut column) => column[*keeper] += update.value.to_f32(),
                        DbColumn::Texts(ref mut _column) => return Err(ServerError::Query),
                    }
                },
                UpdateOp::TimesEquals => {
                    match table.columns.get_mut(&update.attribute).unwrap() {
                        DbColumn::Ints(ref mut column) => column[*keeper] *= update.value.to_i32(),
                        DbColumn::Floats(ref mut column) => column[*keeper] *= update.value.to_f32(),
                        DbColumn::Texts(ref mut column) => column[*keeper] = update.value.clone(),
                    }
                },
                UpdateOp::Append => {
                    match table.columns.get_mut(&update.attribute).unwrap() {
                        DbColumn::Ints(ref mut _column) => return Err(ServerError::Query),
                        DbColumn::Floats(ref mut _column) => return Err(ServerError::Query),
                        DbColumn::Texts(ref mut column) => column[*keeper].push(update.value.as_str())?,
                    }
                },
                UpdateOp::Prepend => {
                    match table.columns.get_mut(&update.attribute).unwrap() {
                        DbColumn::Ints(ref mut _column) => return Err(ServerError::Query),
                        DbColumn::Floats(ref mut _column) => return Err(ServerError::Query),
                        DbColumn::Texts(ref mut column) => {
                            let mut new = update.value.clone();
                            new.push(column[*keeper].as_str())?;
                            column[*keeper] = new;
                        },
                    }
                },
            }
        }
    }

    Ok(
        table.subtable_from_indexes(&keepers, &KeyString::from("RESULT"))
    )
}

fn execute_insert_query(query: Query, database: Arc<Database>) -> Result<EZTable, ServerError> {
    let tables = database.buffer_pool.tables.read().unwrap();
    let table = tables.get(&query.table).unwrap().read().unwrap();
    let keepers = filter_keepers(&query, &table)?;

    Ok(
        table.subtable_from_indexes(&keepers, &KeyString::from("RESULT"))
    )
}

fn execute_select_query(query: Query, database: Arc<Database>) -> Result<EZTable, ServerError> {
    let tables = database.buffer_pool.tables.read().unwrap();
    let table = tables.get(&query.table).unwrap().read().unwrap();

    let keepers = filter_keepers(&query, &table)?;

    Ok(
        table.subtable_from_indexes(&keepers, &KeyString::from("RESULT"))
    )
}


pub fn filter_keepers(query: &Query, table: &EZTable) -> Result<Vec<usize>, ServerError> {
    let mut indexes = Vec::new();

    match query.primary_keys {
        RangeOrListorAll::Range(ref start, ref stop) => {
            match &table.columns[&table.get_primary_key_col_index()] {
                DbColumn::Ints(column) => {
                    let first = match column.binary_search(&start.to_i32()) {
                        Ok(x) => x,
                        Err(x) => x,
                    };
                    let last = match column.binary_search(&stop.to_i32()) {
                        Ok(x) => x,
                        Err(x) => x,
                    };
                    indexes = (first..last).collect();
                },
                DbColumn::Texts(column) => {
                    let first = match column.binary_search(&start) {
                        Ok(x) => x,
                        Err(x) => x,
                    };
                    let last = match column.binary_search(&stop) {
                        Ok(x) => x,
                        Err(x) => x,
                    };
                    indexes = (first..last).collect();
                },
                DbColumn::Floats(_n) => {
                    unreachable!("There should never be a float primary key")
                },
            }
        },
        RangeOrListorAll::List(ref keys) => {
            match &table.columns[&table.get_primary_key_col_index()] {
                DbColumn::Ints(column) => {
                    if keys.len() > column.len() {
                        return Err(ServerError::Query)
                    }
                    let mut keys = keys.clone();
                    keys.sort();
                    let mut key_index: usize = 0;
                    for index in 0..column.len() {
                        if column[index] == keys[key_index].to_i32() {
                            indexes.push(index);
                            key_index += 1;
                        }
                    }
                },
                DbColumn::Floats(_) => {
                    unreachable!("There should never be a float primary key")
                },
                DbColumn::Texts(column) => {
                    if keys.len() > column.len() {
                        return Err(ServerError::Query)
                    }
                    let mut keys = keys.clone();
                    keys.sort();
                    let mut key_index = 0;
                    for index in 0..column.len() {
                        if column[index] == keys[key_index] {
                            indexes.push(index);
                            key_index += 1;
                        }
                    }
                },
            }
        },
        RangeOrListorAll::All => indexes = (0..table.len()).collect(),

    } // Match primary keys

    let mut keepers = Vec::<usize>::new();
    let mut current_op = Operator::OR;
    for condition in query.conditions.iter() {
        match condition {
            OpOrCond::Op(op) => current_op = *op,
            OpOrCond::Cond(cond) => {
                if !table.columns.contains_key(&cond.attribute) {
                    return Err(ServerError::Query)
                }
                let column = &table.columns[&cond.attribute];
                if current_op == Operator::OR {
                    for index in &indexes {
                        match &cond.test {
                            Test::Equals(bar) => {
                                match column {
                                    DbColumn::Ints(col) => if col[*index] == bar.to_i32() {keepers.push(*index)},
                                    DbColumn::Floats(col) => if col[*index] == bar.to_f32() {keepers.push(*index)},
                                    DbColumn::Texts(col) => if col[*index] == *bar {keepers.push(*index)},
                                }
                            },
                            Test::NotEquals(bar) => {
                                match column {
                                    DbColumn::Ints(col) => if col[*index] != bar.to_i32() {keepers.push(*index)},
                                    DbColumn::Floats(col) => if col[*index] != bar.to_f32() {keepers.push(*index)},
                                    DbColumn::Texts(col) => if col[*index] != *bar {keepers.push(*index)},
                                }
                            },
                            Test::Less(bar) => {
                                match column {
                                    DbColumn::Ints(col) => if col[*index] < bar.to_i32() {keepers.push(*index)},
                                    DbColumn::Floats(col) => if col[*index] < bar.to_f32() {keepers.push(*index)},
                                    DbColumn::Texts(col) => if col[*index] < *bar {keepers.push(*index)},
                                }
                            },
                            Test::Greater(bar) => {
                                match column {
                                    DbColumn::Ints(col) => if col[*index] > bar.to_i32() {keepers.push(*index)},
                                    DbColumn::Floats(col) => if col[*index] > bar.to_f32() {keepers.push(*index)},
                                    DbColumn::Texts(col) => if col[*index] > *bar {keepers.push(*index)},
                                }
                            },
                            Test::Starts(bar) => {
                                match column {
                                    DbColumn::Texts(col) => if col[*index].as_str().starts_with(bar.as_str()) {keepers.push(*index)},
                                    _ => return Err(ServerError::Query),
                                }
                            },
                            Test::Ends(bar) => {
                                match column {
                                    DbColumn::Texts(col) => if col[*index].as_str().ends_with(bar.as_str()) {keepers.push(*index)},
                                    _ => return Err(ServerError::Query),
                                }
                            },
                            Test::Contains(bar) => {
                                match column {
                                    DbColumn::Texts(col) => if col[*index].as_str().contains(bar.as_str()) {keepers.push(*index)},
                                    _ => return Err(ServerError::Query),
                                }
                            },
                        }
                    }
                } else {
                    let mut losers = Vec::new();
                    for keeper in &keepers {
                        match &cond.test {
                            Test::Equals(bar) => {
                                match column {
                                    DbColumn::Ints(col) => if col[*keeper] == bar.to_i32() {losers.push(*keeper)},
                                    DbColumn::Floats(col) => if col[*keeper] == bar.to_f32() {losers.push(*keeper)},
                                    DbColumn::Texts(col) => if col[*keeper] == *bar {losers.push(*keeper)},
                                }
                            },
                            Test::NotEquals(bar) => {
                                match column {
                                    DbColumn::Ints(col) => if col[*keeper] != bar.to_i32() {losers.push(*keeper)},
                                    DbColumn::Floats(col) => if col[*keeper] != bar.to_f32() {losers.push(*keeper)},
                                    DbColumn::Texts(col) => if col[*keeper] != *bar {losers.push(*keeper)},
                                }
                            },
                            Test::Less(bar) => {
                                match column {
                                    DbColumn::Ints(col) => if col[*keeper] < bar.to_i32() {losers.push(*keeper)},
                                    DbColumn::Floats(col) => if col[*keeper] < bar.to_f32() {losers.push(*keeper)},
                                    DbColumn::Texts(col) => if col[*keeper] < *bar {losers.push(*keeper)},
                                }
                            },
                            Test::Greater(bar) => {
                                match column {
                                    DbColumn::Ints(col) => if col[*keeper] > bar.to_i32() {losers.push(*keeper)},
                                    DbColumn::Floats(col) => if col[*keeper] > bar.to_f32() {losers.push(*keeper)},
                                    DbColumn::Texts(col) => if col[*keeper] > *bar {losers.push(*keeper)},
                                }
                            },
                            Test::Starts(bar) => {
                                match column {
                                    DbColumn::Texts(col) => if col[*keeper].as_str().starts_with(bar.as_str()) {losers.push(*keeper)},
                                    _ => return Err(ServerError::Query),
                                }
                            },
                            Test::Ends(bar) => {
                                match column {
                                    DbColumn::Texts(col) => if col[*keeper].as_str().ends_with(bar.as_str()) {losers.push(*keeper)},
                                    _ => return Err(ServerError::Query),
                                }
                            },
                            Test::Contains(bar) => {
                                match column {
                                    DbColumn::Texts(col) => if col[*keeper].as_str().contains(bar.as_str()) {losers.push(*keeper)},
                                    _ => return Err(ServerError::Query),
                                }
                            },
                        }
                    }
                    remove_indices(&mut keepers, &losers);
                }
            },
        }
    }

    Ok(keepers)
}


#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    #[should_panic]
    fn test_Condition_new_fail() {
        let test = Condition::new("att", "is", "500").unwrap();
    }

    #[test]
    fn test_Condition_new_pass() {
        let test = Condition::new("att", "equals", "500").unwrap();
    }

    #[test]
    fn test_Condition_from_str() {
        let test = Condition::from_str("\"att and other\" equals 500").unwrap();
        dbg!(test);
    }

    #[test]
    fn test_parse_contained_token() {
        let text = "hello. (this part is contained). \"This one is not\"";
        let output= parse_contained_token(text, '(', ')').unwrap();
        assert_eq!(output, "this part is contained");
        let second = parse_contained_token(text, '"', '"').unwrap();
        assert_eq!(second, "This one is not");

    }

    #[test]
    fn test_parse_query() {
        let query = "SELECT;
        products;
        0113000, 0113034, 0113035, 0113500;
        (price less 500)
        AND
        (price greater 200)
        AND
        (location equals lag15)";
        let query = parse_EZQL(query).unwrap();

        let test_query = Query {
            table: KeyString::from("products"),
            query_type: QueryType::SELECT,
            primary_keys: RangeOrListorAll::List(vec![
                KeyString::from("0113000"),
                KeyString::from("0113034"),
                KeyString::from("0113035"),
                KeyString::from("0113500"),
            ]),
            conditions: vec![
                OpOrCond::Cond(
                    Condition {
                        attribute: KeyString::from("price"),
                        test: Test::Less(KeyString::from("500")),
                    },
                ),
                OpOrCond::Op(Operator::AND),
                OpOrCond::Cond(
                    Condition {
                        attribute: KeyString::from("price"),
                        test: Test::Greater(KeyString::from("200")),
                    },
                ),
                OpOrCond::Op(Operator::AND),
                OpOrCond::Cond(
                    Condition {
                        attribute: KeyString::from("location"),
                        test: Test::Equals(KeyString::from("lag15")),
                    },
                )
            ],
            updates: Vec::new(),
        };

        dbg!(&query[0]);

        assert_eq!(query[0], test_query);
        dbg!(query);
    }

    #[test]
    fn test_updates() {
        let query = r#"UPDATE;
            Products;
            *;
            (name contains "steel screw")
            OR
            (name contains "wood screw")
            TO;
            (price += 400)
            ("in stock" *= 1.15)
            (name append " *Updated")"#;
        let parsed = parse_EZQL(query).unwrap();

        println!("{}", parsed[0]);
    }



}