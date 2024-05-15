use std::{collections::HashMap, fmt::Display, slice::Chunks, sync::Arc};

use crate::{db_structure::{remove_indices, subtable_from_keys, DbColumn, EZTable, KeyString, StrictError}, networking_utilities::{print_sep_list, ServerError}, server_networking::Database};

use crate::PATH_SEP;

#[derive(Debug, PartialEq)]
pub enum QueryError {
    InvalidQueryType,
    InvalidConditionFormat,
    InvalidTest,
    InvalidTO,
    InvalidUpdate,
    TableNameTooLong,
    Unknown,
    InvalidQueryStructure,
}

impl Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::InvalidQueryType => write!(f, "InvalidQueryType,"),
            QueryError::InvalidConditionFormat => write!(f, "    InvalidConditionFormat,"),
            QueryError::InvalidTest => write!(f, "InvalidTest,"),
            QueryError::InvalidTO => write!(f, "InvalidTO,"),
            QueryError::InvalidUpdate => write!(f, "InvalidUpdate,"),
            QueryError::TableNameTooLong => write!(f, "TableNameTooLong,"),
            QueryError::Unknown => write!(f, "Unknown,"),
            QueryError::InvalidQueryStructure => write!(f, "InvalidQueryStructure,"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Default)]
pub struct Join {
    pub table: KeyString,
    pub join_column: (KeyString, KeyString),
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Default)]
pub struct Inserts {
    pub value_columns: Vec<KeyString>,
    pub new_values: String,
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Statistic{
    SUM(KeyString),
    MEAN(KeyString),
    MODE(KeyString),
    STDEV(KeyString),
}

impl Display for Statistic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Statistic::SUM(x) => write!(f, "(SUM {x})"),
            Statistic::MEAN(x) => write!(f, "(MEAN {x})"),
            Statistic::MODE(x) => write!(f, "(MODE {x})"),
            Statistic::STDEV(x) => write!(f, "(STDEV {x})"),
        }
    }
}

impl Default for Statistic {
    fn default() -> Self {
        Statistic::SUM(KeyString::from("id"))
    }
}

impl Statistic {
    pub fn from_str(s: &str) -> Result<Statistic, QueryError> {
        let split = s.split_whitespace();
        if split.count() != 2 {
            return Err(QueryError::InvalidQueryStructure)
        } else {
            let mut split = s.split_whitespace();
            let first = split.next().unwrap();
            let second = split.next().unwrap();
            match first {
                "SUM" => Ok(Statistic::SUM(KeyString::from(second))),
                "MEAN" => Ok(Statistic::MEAN(KeyString::from(second))),
                "MODE" => Ok(Statistic::MODE(KeyString::from(second))),
                "STDEV" => Ok(Statistic::STDEV(KeyString::from(second))),
                _ => return Err(QueryError::InvalidQueryStructure),
            }
        }

    }
}

/// A database query that has already been parsed from EZQL (see handlers.rs)
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct Query {
    pub table: KeyString,
    pub query_type: QueryType,
    pub primary_keys: RangeOrListorAll,
    pub conditions: Vec<OpOrCond>,
    pub updates: Vec<Update>,
    pub inserts: Inserts,
    pub join: Join,
    pub summary: Vec<Statistic>,
}

// INSERT(table_name: products, value_columns: (id, stock, location, price), new_values: ((0113035, 500, LAG15, 995), (0113000, 100, LAG30, 495)))
// SELECT(primary_keys: *, table_name: products, conditions: ((price greater-than 500) AND (stock less-than 1000)))
// UPDATE(table_name: products, primary_keys: (0113035, 0113000), conditions: ((id starts-with 011)), updates: ((price += 100), (stock -= 100)))
// DELETE(primary_keys: *, table_name: products, conditions: ((price greater-than 500) AND (stock less-than 1000)))

// LEFT_JOIN(left_table: products, right_table: warehouses, match_columns: (location, id), primary_keys: 0113000..18572054)

impl Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {

        let mut printer = String::new();
        match self.query_type {
            QueryType::SELECT => {
                printer.push_str(&format!("SELECT(table_name: {}, primary_keys: {}, conditions: ({}))",
                        self.table,
                        self.primary_keys,
                        print_sep_list(&self.conditions, " "),
                ));

            },
            QueryType::LEFT_JOIN | QueryType::INNER_JOIN | QueryType::FULL_JOIN | QueryType::RIGHT_JOIN => {
                printer.push_str(&format!("{}(left_table: {}, right_table: {}, primary_keys: {}, match_columns: ({}, {}))",
                        self.query_type,
                        self.table,
                        self.join.table,
                        self.primary_keys,
                        self.join.join_column.0,
                        self.join.join_column.1,
                ));
            },
            QueryType::UPDATE => {
                printer.push_str(&format!("UPDATE(table_name: {}, primary_keys: {}, conditions: ({}), updates: ({}))",
                        self.table,
                        self.primary_keys,
                        print_sep_list(&self.conditions, " "),
                        print_sep_list(&self.updates, ", "),
                ));
            },
            QueryType::INSERT => {

                let new_values = self.inserts.new_values.clone().replace(';', ", ");
                let mut temp = String::from("");
                for line in new_values.lines() {
                    temp.push_str(&format!("({line}), "));
                }
                temp.pop();
                temp.pop();
                

                printer.push_str(&format!("INSERT(table_name: {}, value_columns: ({}), new_values: ({}))",
                        self.table,
                        print_sep_list(&self.inserts.value_columns, ", "),
                        temp,
                ));
            },
            QueryType::DELETE => {
                printer.push_str(&format!("DELETE(table_name: {}, primary_keys: {}, conditions: ({}))",
                        self.table,
                        self.primary_keys,
                        print_sep_list(&self.conditions, " "),
                ));
            },
            QueryType::SUMMARY => {
                printer.push_str(&format!("SUMMARY(table_name: {}, columns: ({}))",
                        self.table,
                        print_sep_list(&self.summary, ", "),
                ));
            },
        }


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
            inserts: Inserts::default(),
            join: Join::default(),
            summary: Vec::new(),
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
            UpdateOp::MinusEquals => "-=",
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
    MinusEquals,
    TimesEquals,
    Append,
    Prepend,
}

impl UpdateOp {
    fn from_str(s: &str) -> Result<Self, QueryError> {
        match s {
            "=" => Ok(UpdateOp::Assign),
            "+=" => Ok(UpdateOp::PlusEquals),
            "-=" => Ok(UpdateOp::MinusEquals),
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
    SUMMARY,
}

impl Display for QueryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryType::SELECT => write!(f, "SELECT"),
            QueryType::LEFT_JOIN => write!(f, "LEFT_JOIN"),
            QueryType::INNER_JOIN => write!(f, "INNER_JOIN"),
            QueryType::RIGHT_JOIN => write!(f, "RIGHT_JOIN"),
            QueryType::FULL_JOIN => write!(f, "FULL_JOIN"),
            QueryType::UPDATE => write!(f, "UPDATE"),
            QueryType::INSERT => write!(f, "INSERT"),
            QueryType::DELETE => write!(f, "DELETE"),
            QueryType::SUMMARY => write!(f, "SUMMARY"),
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

impl Display for RangeOrListorAll {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut printer = String::new();
        match &self {
            RangeOrListorAll::Range(start, stop) => printer.push_str(&format!("{}..{}", start, stop)),
            RangeOrListorAll::List(list) => {
                printer.push('(');
                printer.push_str(&print_sep_list(list, ", "));
                printer.push(')');
            },
            RangeOrListorAll::All => printer.push_str("*"),
        };
        write!(f, "{}", printer)
    }
}

/// Represents the condition a item must pass to be included in the result
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Condition {
    pub attribute: KeyString,
    pub test: Test,
}

impl Display for Condition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} - {}", self.attribute, self.test)
    }
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
            OpOrCond::Cond(cond) => write!(f, "({} {})", cond.attribute, cond.test.to_string()),
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
            "less-than" => Test::Less(KeyString::from(bar)),
            "greater-than" => Test::Greater(KeyString::from(bar)),
            "starts-with" => Test::Starts(KeyString::from(bar)),
            "ends-with" => Test::Ends(KeyString::from(bar)),
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
    Any,
    LeftJoin,
}

impl Expect {
    pub fn from_string(s: &str) -> Result<Expect, QueryError> {
        match s {
            "QueryType" | "query_type" => Ok(Expect::QueryType),
            "TableName" | "table_name" => Ok(Expect::TableName),
            "PrimaryKeys" | "primary_keys" => Ok(Expect::PrimaryKeys),
            "Conditions" | "conditions" => Ok(Expect::Conditions),
            "Updates" | "updates" => Ok(Expect::Updates),
            "Inserts" | "inserts" => Ok(Expect::Inserts),
            "Any" | "any"=> Ok(Expect::Any),
            "LeftJoin" | "left_join" => Ok(Expect::LeftJoin),
            _ => Err(QueryError::InvalidQueryStructure)
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            Expect::QueryType => "query_type".to_owned(),
            Expect::TableName => "table_name".to_owned(),
            Expect::PrimaryKeys => "primary_keys".to_owned(),
            Expect::Conditions => "conditions".to_owned(),
            Expect::Updates => "updates".to_owned(),
            Expect::Inserts => "inserts".to_owned(),
            Expect::Any => "any".to_owned(),
            Expect::LeftJoin => "left_join".to_owned(),
        }
    }
}


/*
Alternative EZQL:

EZQL queries are written as functions calls with named parameters. The order of the parameters does not matter.

examples:   
INSERT(table_name: products, value_columns: (id, stock, location, price), new_values: ((0113035, 500, LAG15, 995), (0113000, 100, LAG30, 495)))
SELECT(primary_keys: *, table_name: products, conditions: ((price greater-than 500) AND (stock less-than 1000)))
UPDATE(table_name: products, primary_keys: (0113035, 0113000), conditions: ((id starts-with 011)), updates: ((price += 100), (stock -= 100)))
DELETE(primary_keys: *, table_name: products, conditions: ((price greater-than 500) AND (stock less-than 1000)))

LEFT_JOIN(left_table: products, right_table: warehouses, match_columns: (location, id), primary_keys: 0113000..18572054)

LEFT_JOIN(left_table: products, right_table: warehouses, match_columns: (location, id), primary_keys: 0113000..18572054)
INNER_JOIN(left_table: products, right_table: warehouses, match_columns: (location, id), primary_keys: (0113000, 0113000, 18572054))
FULL_JOIN(left_table: products, right_table: warehouses, match_columns: (location, id), primary_keys: *)
SELECT(table_name: __RESULT__, primary_keys: *, conditions: ())

Chaining queries can be done with the -> operator between subqueries. A chained query uses the table name __RESULT__ to operate on the preivous 
queries result.
Example:
LEFT_JOIN(left_table: products, right_table: warehouses, match_columns: (location, id), primary_keys: 0113000..18572054)
->
SELECT(table_name: __RESULT__, primary_keys: *, conditions: ())

The final query in the chain the the one whose result will be sent back to the caller.

The SUMMARY query is a special query that does not return a table but rather returns a list of SUMMARY on a given table

SUMMARY(table_name: products, columns: ((SUM stock), (MEAN price)))

Refer to the EZ-FORMAT section of the documentation for information of the different data formats of EZDB
*/

pub fn parse_serial_query(query_string: &str) -> Result<Vec<Query>, QueryError> {
    let mut result = Vec::new();

    for subquery in query_string.split("->") {
        result.push(parse_EZQL(subquery)?);
    }

    Ok(result)
}

pub struct ParserState {
    depth: u8,
    stack: Vec<u8>,
    word_buffer: Vec<u8>,

}

#[allow(non_snake_case)]
pub fn parse_EZQL(query_string: &str) -> Result<Query, QueryError> {

    let mut state = ParserState {
        depth: 0,
        stack: Vec::with_capacity(256),
        word_buffer: Vec::with_capacity(64),
    };

    let mut query = Query::new();

    let first_paren = match query_string.find('(') {
        Some(x) => x,
        None => return Err(QueryError::InvalidQueryStructure)
    };

    query.query_type = match &query_string[0..first_paren] {
        "INSERT" => QueryType::INSERT,
        "SELECT" => QueryType::SELECT,
        "UPDATE" => QueryType::UPDATE,
        "DELETE" => QueryType::DELETE,
        "LEFT_JOIN" => QueryType::LEFT_JOIN,
        "FULL_JOIN" => QueryType::FULL_JOIN,
        "INNER_JOIN" => QueryType::INNER_JOIN,
        "SUMMARY" => QueryType::SUMMARY,
        _ => return Err(QueryError::InvalidQueryType),
    };

    let mut args: HashMap<String, Vec<String>> = HashMap::new();
    let mut current_arg = String::new();

    for c in query_string.as_bytes()[first_paren..].iter() {
        match c {
            b'(' | b'[' => {
                state.stack.push(*c);
                state.depth += 1;
            },
            b')' => {
                match state.stack.last() {
                    Some(x) => {
                        if *x == b'(' {
                            state.stack.pop();
                            state.depth -= 1;
                        }
                        else {return Err(QueryError::InvalidQueryStructure)}
                    }
                    None => return Err(QueryError::InvalidQueryStructure)
                }
            },
            b':' => {
                let word = match String::from_utf8(state.word_buffer.clone()) {
                    Ok(s) => s.trim().to_owned(),
                    Err(e) => return Err(QueryError::InvalidQueryStructure),
                };
                if word.len() > 64 {
                    return Err(QueryError::TableNameTooLong)
                }
                current_arg = word;
                state.word_buffer.clear();
                
            }
            b',' => {
                let word = match String::from_utf8(state.word_buffer.clone()) {
                    Ok(s) => s.trim().to_owned(),
                    Err(e) => return Err(QueryError::InvalidQueryStructure),
                };
                state.word_buffer.clear();
                args.entry(current_arg.clone()).and_modify(|n| n.push(word.clone())).or_insert(vec![word.clone()]);
                
            }
            other => {
                state.word_buffer.push(*other);
            }         
        }
    }

    if !state.stack.is_empty() {
        return Err(QueryError::InvalidQueryStructure)
    }

    let word = match String::from_utf8(state.word_buffer.clone()) {
        Ok(s) => s.trim().to_owned(),
        Err(e) => return Err(QueryError::InvalidQueryStructure),
    };
    state.word_buffer.clear();
    args.entry(current_arg.clone()).and_modify(|n| n.push(word.clone())).or_insert(vec![word.clone()]);

    query.table = match args.get("table_name") {
        Some(x) => {
            let x = match x.get(0) {
                Some(n) => n,
                None => return Err(QueryError::InvalidQueryStructure),
            };
            KeyString::from(x.as_str())
        },
        None => {
            match args.get("left_table") {
                Some(x) => match x.get(0) {
                    Some(n) => KeyString::from(n.as_str()),
                    None => return Err(QueryError::InvalidQueryStructure),
                },
                None => return Err(QueryError::InvalidQueryStructure),
            }
        },
    };
    match query.query_type {
        QueryType::INSERT => {

            let value_columns: Vec<KeyString> = match args.get("value_columns") {
                Some(x) => x.iter().map(|n| KeyString::from(n.as_str())).collect(),
                None => return Err(QueryError::InvalidQueryStructure),
            };
            let new_values = match args.get("new_values") {
                Some(x) => x,
                None => return Err(QueryError::InvalidQueryStructure),
            };
            
            if new_values.len() % value_columns.len() != 0 {
                return Err(QueryError::InvalidQueryStructure);
            } else {
                let mut acc = String::with_capacity(2*new_values.len()*new_values[0].len());
                for tuple in new_values.chunks(value_columns.len()) {
                    for value in tuple {
                        acc.push_str(&value);
                        acc.push(';');
                    }
                    acc.pop();
                    acc.push('\n');
                }
                acc.pop();
                query.inserts = Inserts{value_columns: value_columns, new_values: acc};
            }

        },
        QueryType::SELECT | QueryType::UPDATE | QueryType::DELETE => {
            let conditions = match args.get("conditions") {
                Some(x) => {
                    if x.len() != 1 {
                        return Err(QueryError::InvalidQueryStructure)
                    } else {
                        x[0].split_whitespace().collect::<Vec<&str>>()
                    }
                },
                None => return Err(QueryError::InvalidQueryStructure),
            };

            let mut condition_buffer = String::new();
            for condition in conditions {
                match condition {
                    "AND" => {
                        query.conditions.push(OpOrCond::Cond(Condition::from_str(&condition_buffer.trim())?));
                        condition_buffer.clear();
                        query.conditions.push(OpOrCond::Op(Operator::AND));
                    },
                    "OR" => {
                        query.conditions.push(OpOrCond::Cond(Condition::from_str(&condition_buffer.trim())?));
                        condition_buffer.clear();
                        query.conditions.push(OpOrCond::Op(Operator::AND));
                    },
                    x => {
                        condition_buffer.push_str(x);
                        condition_buffer.push(' ');
                    }
                }
            }
            if !condition_buffer.is_empty() {
                query.conditions.push(OpOrCond::Cond(Condition::from_str(&condition_buffer.trim())?));
            }


            let primary_keys = match args.get("primary_keys") {
                Some(x) => x,
                None => return Err(QueryError::InvalidQueryStructure),
            };

            match primary_keys.len() {
                0 => return Err(QueryError::InvalidQueryStructure),
                1 => {
                    match primary_keys[0].as_str() {
                        "*" => query.primary_keys = RangeOrListorAll::All,
                        x => {
                            match x.find("..") {
                                Some(_) => {
                                    let mut split = x.split("..");
                                    let start = match split.next() {
                                        Some(x) => KeyString::from(x),
                                        None => return Err(QueryError::InvalidQueryStructure)
                                    };
                                    let stop = match split.next() {
                                        Some(x) => KeyString::from(x),
                                        None => return Err(QueryError::InvalidQueryStructure)
                                    };
                                    query.primary_keys = RangeOrListorAll::Range(start, stop);
                                },
                                None => {
                                    query.primary_keys = RangeOrListorAll::List(vec![KeyString::from(x)]);
                                }
                            }
                            
                        }
                    }
                },
                _ => {
                    let primary_keys: Vec<KeyString> = primary_keys.iter().map(|n| KeyString::from(n.as_str())).collect();
                    query.primary_keys = RangeOrListorAll::List(primary_keys);
                }
            };

            match query.query_type {
                QueryType::UPDATE => {
                    let updates = match args.get("updates") {
                        Some(x) => x,
                        None => return Err(QueryError::InvalidQueryStructure),
                    };
                    let mut acc = Vec::with_capacity(updates.len());
                    for update in updates {
                        acc.push(Update::from_str(update)?);
                    }
                    query.updates = acc;
                },
                _ => ()
            }

        },
        QueryType::LEFT_JOIN | QueryType::INNER_JOIN | QueryType::RIGHT_JOIN | QueryType::FULL_JOIN => {
            query.join.table = match args.get("right_table") {
                Some(x) => match x.get(0) {
                    Some(n) => KeyString::from(n.as_str()),
                    None => return Err(QueryError::InvalidQueryStructure),
                },
                None => return Err(QueryError::InvalidQueryStructure),
            };

            let primary_keys = match args.get("primary_keys") {
                Some(x) => x,
                None => return Err(QueryError::InvalidQueryStructure),
            };

            match primary_keys.len() {
                0 => return Err(QueryError::InvalidQueryStructure),
                1 => {
                    match primary_keys[0].as_str() {
                        "*" => query.primary_keys = RangeOrListorAll::All,
                        x => {
                            let mut split = x.split("..");
                            let start = match split.next() {
                                Some(x) => KeyString::from(x),
                                None => return Err(QueryError::InvalidQueryStructure)
                            };
                            let stop = match split.next() {
                                Some(x) => KeyString::from(x),
                                None => return Err(QueryError::InvalidQueryStructure)
                            };
                            query.primary_keys = RangeOrListorAll::Range(start, stop);
                        }
                    }
                },
                _ => {
                    let primary_keys: Vec<KeyString> = primary_keys.iter().map(|n| KeyString::from(n.as_str())).collect();
                    query.primary_keys = RangeOrListorAll::List(primary_keys);
                }
            };

            let match_columns: Vec<KeyString> = match args.get("match_columns") {
                Some(x) => x.iter().map(|s| KeyString::from(s.as_str())).collect(),
                None => return Err(QueryError::InvalidQueryStructure),
            };

            if match_columns.len() != 2 {
                return Err(QueryError::InvalidQueryStructure)
            } else {
                query.join.join_column = (match_columns[0], match_columns[1]);
            }



        },
        QueryType::SUMMARY => {
            // SUMMARY(table_name: products, columns: ((SUM stock), (MEAN price)))
            let summary = match args.get("columns") {
                Some(x) => x,
                None => return Err(QueryError::InvalidQueryStructure),
            };

            let mut temp = Vec::with_capacity(summary.len());
            for stat in summary {
                let s = Statistic::from_str(stat)?;
                temp.push(s);
            }

            query.summary = temp;

        },
    }


    Ok(query)

}


pub fn subsplitter<'a>(s: &'a str) -> Vec<Vec<&'a str>> {

    let mut temp = Vec::new();
    for line in s.split(';') {
        temp.push(line.split(',').collect::<Vec<&str>>());
    }

    temp

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
pub fn execute_EZQL_queries(queries: Vec<Query>, database: Arc<Database>) -> Result<Option<EZTable>, ServerError> {

    let mut result_table = None;
    for query in queries.into_iter() {

        match query.query_type {
            QueryType::DELETE => {
                match result_table {
                    Some(mut table) => result_table = execute_delete_query(query, &mut table)?,
                    None => {
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let mut table = tables.get(&query.table).unwrap().write().unwrap();
                        result_table = execute_delete_query(query, &mut table)?;
                        database.buffer_pool.naughty_list.write().unwrap().insert(table.name);
                    },
                }
                
            },
            QueryType::SELECT => {
                match result_table {
                    Some(mut table) => result_table = execute_delete_query(query, &mut table)?,
                    None => {
                        println!("table name: {}", &query.table);
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let table = tables.get(&query.table).unwrap().read().unwrap();
                        result_table = execute_select_query(query, &table)?;
                    },
                }
            },
            QueryType::LEFT_JOIN => {
                match result_table {
                    Some(table) => {
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let right_table = tables.get(&query.join.table).unwrap().read().unwrap();
                        result_table = execute_left_join_query(query, &table, &right_table)?;
                    },
                    None => {
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let left_table = tables.get(&query.table).unwrap().read().unwrap();
                        let right_table = tables.get(&query.join.table).unwrap().read().unwrap();
                        execute_left_join_query(query, &left_table, &right_table)?;
                    },
                }
                
            },
            QueryType::INNER_JOIN => {
                unimplemented!("Inner joins are not yet implemented");
                // execute_inner_join_query(query, database);
            },
            QueryType::RIGHT_JOIN => {
                unimplemented!("Right joins are not yet implemented");

                // execute_right_join_query(query, database);
            },
            QueryType::FULL_JOIN => {
                unimplemented!("Full joins are not yet implemented");

                // execute_full_join_query(query, database);
            },
            QueryType::UPDATE => {
                match result_table {
                    Some(mut table) => result_table = execute_update_query(query, &mut table)?,
                    None => {
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let mut table = tables.get(&query.table).unwrap().write().unwrap();
                        result_table = execute_update_query(query, &mut table)?;
                        database.buffer_pool.naughty_list.write().unwrap().insert(table.name);
                    },
                }
            },
            QueryType::INSERT => {
                match result_table {
                    Some(mut table) => result_table = execute_insert_query(query, &mut table)?,
                    None => {
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let mut table = tables.get(&query.table).unwrap().write().unwrap();
                        result_table = execute_insert_query(query, &mut table)?;
                        database.buffer_pool.naughty_list.write().unwrap().insert(table.name);
                    },
                }
            },
            QueryType::SUMMARY => unimplemented!(),
        }
    }
    Ok(result_table)
}


fn execute_delete_query(query: Query, table: &mut EZTable) -> Result<Option<EZTable>, ServerError> {
    
    let keepers = filter_keepers(&query, &table)?;
    table.delete_by_indexes(&keepers);

    Ok(
        None
    )
}

fn execute_left_join_query(query: Query, left_table: &EZTable, right_table: &EZTable) -> Result<Option<EZTable>, ServerError> {
    
    let filtered_indexes = keys_to_indexes(&left_table, &query.primary_keys)?;
    let mut filtered_table = left_table.subtable_from_indexes(&filtered_indexes, &KeyString::from("__RESULT__"));

    filtered_table.left_join(&right_table, &query.join.join_column.0)?;

    Ok(Some(filtered_table))
    
}

fn execute_inner_join_query(query: Query, database: Arc<Database>) -> Result<Option<EZTable>, ServerError> {
    let tables = database.buffer_pool.tables.read().unwrap();
    let table = tables.get(&query.table).unwrap().read().unwrap();
    let keepers = filter_keepers(&query, &table)?;

    return Err(ServerError::Unimplemented("inner joins are not yet implemented".to_owned()));
}

fn execute_right_join_query(query: Query, database: Arc<Database>) -> Result<Option<EZTable>, ServerError> {
    let tables = database.buffer_pool.tables.read().unwrap();
    let table = tables.get(&query.table).unwrap().read().unwrap();
    let keepers = filter_keepers(&query, &table)?;

    return Err(ServerError::Unimplemented("right joins are not yet implemented".to_owned()));
}

fn execute_full_join_query(query: Query, database: Arc<Database>) -> Result<Option<EZTable>, ServerError> {
    let tables = database.buffer_pool.tables.read().unwrap();
    let table = tables.get(&query.table).unwrap().read().unwrap();
    let keepers = filter_keepers(&query, &table)?;

    return Err(ServerError::Unimplemented("full joins are not yet implemented".to_owned()));
}

// pub struct Update {
//     attribute: KeyString,
//     Operator: UpdateOp,
//     Value: KeyString,
// }

fn execute_update_query(query: Query, table: &mut EZTable) -> Result<Option<EZTable>, ServerError> {
    
    let keepers = filter_keepers(&query, &table)?;

    for keeper in &keepers {
        for update in &query.updates{
            if !table.columns.contains_key(&update.attribute) {
                return Err(ServerError::Query(format!("Table does not contain column {}", update.attribute)))
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
                        DbColumn::Texts(ref mut _column) => return Err(ServerError::Query("'+=' operator cannot be performed on text data".to_owned())),
                    }
                },
                UpdateOp::MinusEquals => {
                    match table.columns.get_mut(&update.attribute).unwrap() {
                        DbColumn::Ints(ref mut column) => column[*keeper] -= update.value.to_i32(),
                        DbColumn::Floats(ref mut column) => column[*keeper] -= update.value.to_f32(),
                        DbColumn::Texts(ref mut _column) => return Err(ServerError::Query("'-=' operator cannot be performed on text data".to_owned())),
                    }
                }
                UpdateOp::TimesEquals => {
                    match table.columns.get_mut(&update.attribute).unwrap() {
                        DbColumn::Ints(ref mut column) => column[*keeper] *= update.value.to_i32(),
                        DbColumn::Floats(ref mut column) => column[*keeper] *= update.value.to_f32(),
                        DbColumn::Texts(ref mut column) => column[*keeper] = update.value.clone(),
                    }
                },
                UpdateOp::Append => {
                    match table.columns.get_mut(&update.attribute).unwrap() {
                        DbColumn::Ints(ref mut _column) => return Err(ServerError::Query("'append' operator can only be performed on text data".to_owned())),
                        DbColumn::Floats(ref mut _column) => return Err(ServerError::Query("'append' operator can only be performed on text data".to_owned())),
                        DbColumn::Texts(ref mut column) => column[*keeper].push(update.value.as_str())?,
                    }
                },
                UpdateOp::Prepend => {
                    match table.columns.get_mut(&update.attribute).unwrap() {
                        DbColumn::Ints(ref mut _column) => return Err(ServerError::Query("'prepend' operator can only be performed on text data".to_owned())),
                        DbColumn::Floats(ref mut _column) => return Err(ServerError::Query("'prepend' operator can only be performed on text data".to_owned())),
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
        None    
    )
}

fn execute_insert_query(query: Query, table: &mut EZTable) -> Result<Option<EZTable>, ServerError> {

    table.insert(query.inserts)?;

    Ok(
        None
    )
}

fn execute_select_query(query: Query, table: &EZTable) -> Result<Option<EZTable>, ServerError> {

    let keepers = filter_keepers(&query, &table)?;

    Ok(
        Some(table.subtable_from_indexes(&keepers, &KeyString::from("RESULT")))
    )
}

fn keys_to_indexes(table: &EZTable, keys: &RangeOrListorAll) -> Result<Vec<usize>, StrictError> {
    let mut indexes = Vec::new();

    match keys {
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
                        return Err(StrictError::Query("There are more keys requested than there are indexes to get".to_owned()))
                    }
                    let mut keys = keys.clone();
                    keys.sort();
                    let mut key_index: usize = 0;
                    for index in 0..keys.len() {
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
                        return Err(StrictError::Query("There are more keys requested than there are indexes to get".to_owned()))
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
    };

    Ok(indexes)
}


pub fn filter_keepers(query: &Query, table: &EZTable) -> Result<Vec<usize>, ServerError> {
    let indexes = keys_to_indexes(table, &query.primary_keys)?;
    
    if query.conditions.len() == 0 {
        return Ok(indexes);
    }
    let mut keepers = Vec::<usize>::new();
    let mut current_op = Operator::OR;
    for condition in query.conditions.iter() {
        match condition {
            OpOrCond::Op(op) => current_op = *op,
            OpOrCond::Cond(cond) => {
                if !table.columns.contains_key(&cond.attribute) {
                    return Err(ServerError::Query(format!("table does not contain column {}", cond.attribute)))
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
                                    _ => return Err(ServerError::Query("Can only filter by 'starts-with' on text values".to_owned())),
                                }
                            },
                            Test::Ends(bar) => {
                                match column {
                                    DbColumn::Texts(col) => if col[*index].as_str().ends_with(bar.as_str()) {keepers.push(*index)},
                                    _ => return Err(ServerError::Query("Can only filter by 'ends-with' on text values".to_owned())),
                                }
                            },
                            Test::Contains(bar) => {
                                match column {
                                    DbColumn::Texts(col) => if col[*index].as_str().contains(bar.as_str()) {keepers.push(*index)},
                                    _ => return Err(ServerError::Query("Can only filter by 'contains' on text values".to_owned())),
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
                                    _ => return Err(ServerError::Query("Can only filter by 'starts-with' on text values".to_owned())),
                                }
                            },
                            Test::Ends(bar) => {
                                match column {
                                    DbColumn::Texts(col) => if col[*keeper].as_str().ends_with(bar.as_str()) {losers.push(*keeper)},
                                    _ => return Err(ServerError::Query("Can only filter by 'ends-with' on text values".to_owned())),
                                }
                            },
                            Test::Contains(bar) => {
                                match column {
                                    DbColumn::Texts(col) => if col[*keeper].as_str().contains(bar.as_str()) {losers.push(*keeper)},
                                    _ => return Err(ServerError::Query("Can only filter by 'contains' on text values".to_owned())),
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


#[allow(non_snake_case)]
#[allow(unused)]
#[cfg(test)]
mod tests {

    // INSERT(table_name: products, value_columns: (id, stock, location, price), new_values: ((0113035, 500, LAG15, 995), (0113000, 100, LAG30, 495)))
    // SELECT(primary_keys: *, table_name: products, conditions: ((price greater-than 500) AND (stock less-than 1000)))
    // UPDATE(table_name: products, primary_keys: (0113035, 0113000), conditions: ((id starts-with 011)), updates: ((price += 100), (stock -= 100)))
    // DELETE(primary_keys: *, table_name: products, conditions: ((price greater-than 500) AND (stock less-than 1000)))
    // LEFT_JOIN(left_table: products, right_table: warehouses, match_columns: (location, id), primary_keys: 0113000..18572054)
    // SUMMARY(table_name: products, columns: ((SUM stock), (MEAN price)))


    use std::default;

    use super::*;

    #[test]
    fn test_Condition_new_fail() {
        let test = Condition::new("att", "is", "500");
        assert!(test.is_err());
    }

    #[test]
    fn test_Condition_new_pass() {
        let test = Condition::new("att", "equals", "500").unwrap();
    }

    #[test]
    fn test_Condition_from_str() {
        let test = Condition::from_str("\"att and other\" equals 500").unwrap();
        println!("{}", test);
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
        let INSERT_query_string =  "INSERT(table_name: products, value_columns: (id, stock, location, price), new_values: ((0113035, 500, LAG15, 995), (0113000, 100, LAG30, 495)))";
        let SELECT_query_string = "SELECT(table_name: products, primary_keys: (0113000, 0113034, 0113035, 0113500), conditions: ((price less-than 500) AND (price greater-than 200) AND (location equals lag15)))";
        let UPDATE_query_string = "UPDATE(table_name: products, primary_keys: (0113035, 0113000), conditions: ((id starts-with 011)), updates: ((price += 100), (stock -= 100)))";
        let DELETE_query_string = "DELETE(table_name: products, primary_keys: *, conditions: ((price greater-than 500) AND (stock less-than 1000)))";
        let LEFT_JOIN_query_string = "LEFT_JOIN(left_table: products, right_table: warehouses, primary_keys: 0113000..18572054, match_columns: (location, id))";
        let SUMMARY_query_string = "SUMMARY(table_name: products, columns: ((SUM stock), (MEAN price)))";
        
        let INSERT_query = parse_EZQL(INSERT_query_string).unwrap();
        let SELECT_query = parse_EZQL(SELECT_query_string).unwrap();
        let UPDATE_query = parse_EZQL(UPDATE_query_string).unwrap();
        let DELETE_query = parse_EZQL(DELETE_query_string).unwrap();
        let LEFT_JOIN_query = parse_EZQL(LEFT_JOIN_query_string).unwrap();
        let SUMMARY_query = parse_EZQL(SUMMARY_query_string).unwrap();

        println!("{}", &INSERT_query);
        println!("{}", INSERT_query_string);
        println!();

        println!("{}", &SELECT_query);
        println!("{}", SELECT_query_string);
        println!();

        println!("{}", &UPDATE_query);
        println!("{}", UPDATE_query_string);
        println!();

        println!("{}", &DELETE_query);
        println!("{}", DELETE_query_string);
        println!();

        println!("{}", &LEFT_JOIN_query);
        println!("{}", LEFT_JOIN_query_string);
        println!();

        println!("{}", &SUMMARY_query);
        println!("{}", SUMMARY_query_string);
        println!();
        

        assert_eq!(INSERT_query.to_string(), INSERT_query_string);
        assert_eq!(SELECT_query.to_string(), SELECT_query_string);
        assert_eq!(DELETE_query.to_string(), DELETE_query_string);
        assert_eq!(UPDATE_query.to_string(), UPDATE_query_string);
        assert_eq!(LEFT_JOIN_query.to_string(), LEFT_JOIN_query_string);
        assert_eq!(SUMMARY_query.to_string(), SUMMARY_query_string);
    }

    #[test]
    fn test_updates() {
        let query = "UPDATE(table_name: products, primary_keys: (0113035, 0113000), conditions: ((id starts-with 011)), updates: ((price += 100), (stock -= 100)))";
        
        let parsed = parse_serial_query(query).unwrap();

        println!("{}", parsed[0]);
    }

    #[test]
    fn test_SELECT() {
        let table_string = std::fs::read_to_string(format!("test_files{PATH_SEP}good_csv.txt")).unwrap();
        let table = EZTable::from_csv_string(&table_string, "good_csv", "test").unwrap();
        let query = "SELECT(primary_keys: (0113000), table_name: good_csv, conditions: ())";
        let parsed = parse_serial_query(query).unwrap();
        let result = execute_select_query(parsed[0].clone(), &table).unwrap().unwrap();
        println!("{}", result);
        assert_eq!("heiti,t-N;magn,i-N;vnr,i-P\nundirlegg2;100;113000\nundirlegg;200;113035\nflísalím;42;18572054", result.to_string());
    }

    #[test]
    fn test_LEFT_JOIN() {

        let left_string = std::fs::read_to_string(format!("test_files{PATH_SEP}employees.csv")).unwrap();
        let right_string = std::fs::read_to_string(format!("test_files{PATH_SEP}departments.csv")).unwrap();

        let mut left_table = EZTable::from_csv_string(&left_string, "employees", "test").unwrap();
        let right_table = EZTable::from_csv_string(&right_string, "departments", "test").unwrap();

        println!("{}", left_table);
        println!("{}", right_table);
        
        let query_string = "LEFT_JOIN(left_table: employees, right_table: departments, match_columns: (department, department), primary_keys: *)";
        let query = parse_serial_query(query_string).unwrap();
        
        println!("{}", query[0]);
        let actual = execute_left_join_query(query[0].clone(), &left_table, &right_table).unwrap().unwrap();
        println!("{}", actual);

        let expected = "#employees,i-N;budget,i-N;department,t-N;employee_id,i-P;location,t-N;name,t-N;role,t-N\n2;100000;IT;1;'third floor';jim;engineer\n\n1;100;Sales;2;'first floor';jeff;Manager\n2;100000;IT;3;'third floor';bob;engineer\n10;2342;QA;4;'second floor';John;tester";
        
        // assert_eq!(actual.to_string(), expected);
    }

    #[test]
    fn test_INNER_JOIN() {

    }

    #[test]
    fn test_RIGHT_JOIN() {

    }

    #[test]
    fn test_FULL_JOIN() {

    }

    #[test]
    fn test_UPDATE() {
        let query = "UPDATE(table_name: products, primary_keys: *, conditions: ((id starts-with 011)), updates: ((price += 100), (stock -= 100)))";
        let parsed = parse_EZQL(query).unwrap();
        let products = std::fs::read_to_string(format!("test_files{PATH_SEP}products.csv")).unwrap();
        let mut table = EZTable::from_csv_string(&products, "products", "test").unwrap();
        println!("before:\n{}", table);
        println!();
        execute_update_query(parsed, &mut table).unwrap();
        println!("after:\n{}", table);

        let expected_table = "id,t-P;location,t-F;price,f-N;stock,i-N\n0113446;LAG12;2600;0\n18572054;LAG12;4500;42";
        assert_eq!(table.to_string(), expected_table);
    }

    #[test]
    fn test_INSERT() {
        let query = "INSERT(table_name: products, value_columns: (id, stock, location, price), new_values: ((0113035, 500, LAG15, 995), (0113000, 100, LAG30, 495)))";
        let parsed = parse_EZQL(query).unwrap();
        let products = std::fs::read_to_string(format!("test_files{PATH_SEP}products.csv")).unwrap();
        let mut table = EZTable::from_csv_string(&products, "products", "test").unwrap();
        println!("before:\n{}", table);
        println!();
        execute_insert_query(parsed, &mut table).unwrap();
        println!("after:\n{}", table);
        let expected_table = "id,t-P;location,t-F;price,f-N;stock,i-N\n0113000;LAG30;495;100\n0113035;LAG15;995;500\n0113446;LAG12;2500;100\n18572054;LAG12;4500;42";
        assert_eq!(table.to_string(), expected_table);
    }

    #[test]
    fn test_DELETE() {
        let query = "DELETE(table_name: products, primary_keys: *, conditions: ((price greater-than 3000) AND (stock less-than 1000)))";
        let parsed = parse_EZQL(query).unwrap();
        let products = std::fs::read_to_string(format!("test_files{PATH_SEP}products.csv")).unwrap();
        let mut table = EZTable::from_csv_string(&products, "products", "test").unwrap();
        println!("before:\n{}", table);
        println!();
        execute_delete_query(parsed, &mut table).unwrap();
        println!("after:\n{}", table);
        println!();
        let expected = "id,t-P;location,t-F;price,f-N;stock,i-N\n0113446;LAG12;2500;100";
        assert_eq!(table.to_string(), expected);
    }

    #[test]
    fn test_alternate() {
        let good = "SUMMARY(table_name: products, columns: ((SUM stock), (MEAN price)))";
        let good = parse_EZQL(good).unwrap();
        dbg!(good);
        let bad = "SUMMARY(table_name: products, columns: ((SUM stock), (MEAN price))";
        let e = parse_EZQL(bad);
        assert!(e.is_err());
    }


}