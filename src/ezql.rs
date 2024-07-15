use std::{collections::HashMap, fmt::Display, str::FromStr, sync::Arc};

use crate::{db_structure::{remove_indices, DbColumn, EZTable, KeyString, StrictError}, networking_utilities::{mean_f32_slice, mean_i32_slice, median_f32_slice, median_i32_slice, mode_i32_slice, mode_string_slice, print_sep_list, stdev_f32_slice, stdev_i32_slice, sum_f32_slice, sum_i32_slice, ServerError}, server_networking::Database};

use crate::PATH_SEP;

#[derive(Debug, PartialEq)]
pub enum QueryError {
    InvalidQuery,
    InvalidConditionFormat,
    InvalidTest,
    InvalidTO,
    InvalidUpdate,
    TableNameTooLong,
    Unknown,
    InvalidQueryStructure(String),
}

impl Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::InvalidQuery => write!(f, "InvalidQuery,"),
            QueryError::InvalidConditionFormat => write!(f, "    InvalidConditionFormat,"),
            QueryError::InvalidTest => write!(f, "InvalidTest,"),
            QueryError::InvalidTO => write!(f, "InvalidTO,"),
            QueryError::InvalidUpdate => write!(f, "InvalidUpdate,"),
            QueryError::TableNameTooLong => write!(f, "TableNameTooLong,"),
            QueryError::Unknown => write!(f, "Unknown,"),
            QueryError::InvalidQueryStructure(s) => write!(f, "InvalidQueryStructure because of: {s},"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Default)]
pub struct Join {
    pub table: KeyString,
    pub join_column: (KeyString, KeyString),
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Default, Eq, Ord)]
pub struct Inserts {
    pub value_columns: Vec<KeyString>,
    pub new_values: String,
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub enum Statistic{
    SUM(KeyString),
    MEAN(KeyString),
    MEDIAN(KeyString),
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
            Statistic::MEDIAN(x) => write!(f, "(MEDIAN {x})"),
        }
    }
}

impl Default for Statistic {
    fn default() -> Self {
        Statistic::SUM(KeyString::from("id"))
    }
}

impl FromStr for Statistic {
    type Err = QueryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let split = s.split_whitespace();
        if split.count() != 2 {
            Err(QueryError::InvalidQueryStructure("Statistic must be 2 items separated by whitespace".to_owned()))
        } else {
            let mut split = s.split_whitespace();
            let first = split.next().unwrap();
            let second = split.next().unwrap();
            match first {
                "SUM" => Ok(Statistic::SUM(KeyString::from(second))),
                "MEAN" => Ok(Statistic::MEAN(KeyString::from(second))),
                "MODE" => Ok(Statistic::MODE(KeyString::from(second))),
                "MEDIAN" => Ok(Statistic::MEDIAN(KeyString::from(second))),
                "STDEV" => Ok(Statistic::STDEV(KeyString::from(second))),
                _ => Err(QueryError::InvalidQueryStructure("First Statistic item must be SUM, MEAN, MODE, or STDEV".to_owned())),
            }
        }
    }
}

//  - INSERT(table_name: products, value_columns: (id, stock, location, price), new_values: ((0113035, 500, LAG15, 995), (0113000, 100, LAG30, 495)))
//  - SELECT(table_name: products, primary_keys: *, columns: (price, stock), conditions: ((price greater-than 500) AND (stock less-than 1000)))
//  - UPDATE(table_name: products, primary_keys: (0113035, 0113000), conditions: ((id starts-with 011)), updates: ((price += 100), (stock -= 100)))
//  - DELETE(primary_keys: *, table_name: products, conditions: ((price greater-than 500) AND (stock less-than 1000)))
//  - SUMMARY(table_name: products, columns: ((SUM stock), (MEAN price)))
//  - LEFT_JOIN(left_table: products, right_table: warehouses, match_columns: (location, id), primary_keys: 0113000..18572054)


/// A database query that has already been parsed from EZQL (see EZQL.txt)
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[allow(non_camel_case_types)]
pub enum Query {
    SELECT{table_name: KeyString, primary_keys: RangeOrListOrAll, columns: Vec<KeyString>, conditions: Vec<OpOrCond>},
    LEFT_JOIN{left_table_name: KeyString, right_table_name: KeyString, match_columns: (KeyString, KeyString), primary_keys: RangeOrListOrAll},
    INNER_JOIN,
    RIGHT_JOIN,
    FULL_JOIN,
    UPDATE{table_name: KeyString, primary_keys: RangeOrListOrAll, conditions: Vec<OpOrCond>, updates: Vec<Update>},
    INSERT{table_name: KeyString, inserts: Inserts},
    DELETE{primary_keys: RangeOrListOrAll, table_name: KeyString, conditions: Vec<OpOrCond>},
    SUMMARY{table_name: KeyString, columns: Vec<Statistic>},
}

impl Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {

        let mut printer = String::new();
        match self {
            Query::SELECT { table_name, primary_keys, columns, conditions } => {
                printer.push_str(&format!("SELECT(table_name: {}, primary_keys: {}, columns: {}, conditions: ({}))",
                        table_name,
                        primary_keys,
                        print_sep_list(columns, ", "),
                        print_sep_list(conditions, " "),
                ));

            },
            Query::LEFT_JOIN { left_table_name: left_table, right_table_name: right_table, match_columns, primary_keys } => {
                printer.push_str(&format!("LEFT_JOIN(left_table: {}, right_table: {}, primary_keys: {}, match_columns: ({}, {}))",
                        left_table,
                        right_table,
                        primary_keys,
                        match_columns.0,
                        match_columns.1,
                ));
            },
            Query::UPDATE{ table_name, primary_keys, conditions, updates } => {
                printer.push_str(&format!("UPDATE(table_name: {}, primary_keys: {}, conditions: ({}), updates: ({}))",
                        table_name,
                        primary_keys,
                        print_sep_list(conditions, " "),
                        print_sep_list(updates, ", "),
                ));
            },
            Query::INSERT{ table_name, inserts } => {

                let new_values = inserts.new_values.clone().replace(';', ", ");
                let mut temp = String::from("");
                for line in new_values.lines() {
                    temp.push_str(&format!("({line}), "));
                }
                temp.pop();
                temp.pop();
                

                printer.push_str(&format!("INSERT(table_name: {}, value_columns: ({}), new_values: ({}))",
                        table_name,
                        print_sep_list(&inserts.value_columns, ", "),
                        temp,
                ));
            },
            Query::DELETE { primary_keys, table_name, conditions } => {
                printer.push_str(&format!("DELETE(table_name: {}, primary_keys: {}, conditions: ({}))",
                        table_name,
                        primary_keys,
                        print_sep_list(conditions, " "),
                ));
            },
            Query::SUMMARY { table_name, columns } => {
                printer.push_str(&format!("SUMMARY(table_name: {}, columns: ({}))",
                        table_name,
                        print_sep_list(columns, ", "),
                ));
            },
            _ => unimplemented!("Have no implemented all joins yet")
        }


        write!(f, "{}", printer)
    }

}

impl Default for Query {
    fn default() -> Self {
        Self::new()
    }
}

impl Query {
    pub fn new() -> Self {
        Query::SELECT {
            table_name: KeyString::from("__RESULT__"),
            primary_keys: RangeOrListOrAll::All,
            columns: Vec::new(),
            conditions: Vec::new(),
        }
    }

    pub fn blank(keyword: &str) -> Result<Query, QueryError> {
        match keyword {
            "INSERT" => Ok(Query::INSERT{ table_name: KeyString::new(), inserts: Inserts{value_columns: Vec::new(), new_values: String::new()} }),
            "SELECT" => Ok(Query::SELECT{ table_name: KeyString::new(), primary_keys: RangeOrListOrAll::All, columns: Vec::new(), conditions: Vec::new()  }),
            "UPDATE" => Ok(Query::UPDATE{ table_name: KeyString::new(), primary_keys: RangeOrListOrAll::All, conditions: Vec::new(), updates: Vec::new() }),
            "DELETE" => Ok(Query::DELETE{ table_name: KeyString::new(), primary_keys: RangeOrListOrAll::All, conditions: Vec::new() }),
            "LEFT_JOIN" => Ok(Query::LEFT_JOIN{ left_table_name: KeyString::new(), right_table_name: KeyString::new(), match_columns: (KeyString::new(), KeyString::new()), primary_keys: RangeOrListOrAll::All }),
            "FULL_JOIN" => Ok(Query::FULL_JOIN),
            "INNER_JOIN" => Ok(Query::INNER_JOIN),
            "SUMMARY" => Ok(Query::SUMMARY{ table_name: KeyString::new(), columns: Vec::new() }),
            _ => return Err(QueryError::InvalidQuery),
        }
    }

    pub fn get_primary_keys_ref(&self) -> Option<&RangeOrListOrAll> {
        match self {
            Query::SELECT { table_name: _, primary_keys, columns: _, conditions: _ } => Some(primary_keys),
            Query::LEFT_JOIN { left_table_name: _, right_table_name: _, match_columns: _, primary_keys } => Some(primary_keys),
            Query::UPDATE { table_name: _, primary_keys, conditions: _, updates: _ } => Some(primary_keys),
            Query::DELETE { primary_keys, table_name: _, conditions: _ } => Some(primary_keys),
            _ => None
        }
    }

    pub fn get_table_name(&self) -> KeyString {
        match self {
            Query::SELECT { table_name, primary_keys: _, columns: _, conditions: _ } => *table_name,
            Query::LEFT_JOIN { left_table_name, right_table_name: _, match_columns: _, primary_keys: _ } => *left_table_name,
            Query::UPDATE { table_name, primary_keys: _, conditions: _, updates: _ } => *table_name,
            Query::INSERT { table_name, inserts: _ } => *table_name,
            Query::DELETE { primary_keys: _, table_name, conditions: _ } => *table_name,
            Query::SUMMARY { table_name, columns: _ } => *table_name,
            Query::INNER_JOIN => todo!(),
            Query::RIGHT_JOIN => todo!(),
            Query::FULL_JOIN => todo!(),
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

impl FromStr for Update {
    type Err = QueryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
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
                    inside ^= true;
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

impl Update {

    pub fn blank() -> Self{
        Update {
            attribute: KeyString::new(),
            operator: UpdateOp::Assign,
            value: KeyString::new(),
        }
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
            _ => Err(QueryError::InvalidUpdate),
        }
    }
}




/// This enum represents the possible ways to list primary keys to test. 
/// See EZQL spec for details (handlers.rs).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RangeOrListOrAll {
    Range(KeyString, KeyString),
    List(Vec<KeyString>),
    All,
}

impl Display for RangeOrListOrAll {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut printer = String::new();
        match &self {
            RangeOrListOrAll::Range(start, stop) => printer.push_str(&format!("{}..{}", start, stop)),
            RangeOrListOrAll::List(list) => {
                printer.push('(');
                printer.push_str(&print_sep_list(list, ", "));
                printer.push(')');
            },
            RangeOrListOrAll::All => printer.push('*'),
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
            "less_than" => Test::Less(KeyString::from(bar)),
            "greater_than" => Test::Greater(KeyString::from(bar)),
            "starts_with" => Test::Starts(KeyString::from(bar)),
            "ends_with" => Test::Ends(KeyString::from(bar)),
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
                    inside ^= true;
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
            OpOrCond::Cond(cond) => write!(f, "({} {})", cond.attribute, cond.test),
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
            Test::Less(value) => write!(f, "less_than {}", value),
            Test::Greater(value) => write!(f, "greater_than {}", value),
            Test::Starts(value) => write!(f, "starts_with {}", value),
            Test::Ends(value) => write!(f, "ends_with {}", value),
            Test::Contains(value) => write!(f, "contains {}", value),
        }
    }
}

impl Test {
    pub fn new(input: &str, bar: &str) -> Self {
        match input.to_lowercase().as_str() {
            "equals" => Test::Equals(KeyString::from(bar)),
            "not_equals" => Test::NotEquals(KeyString::from(bar)),
            "less_than" => Test::Less(KeyString::from(bar)),
            "greater_than" => Test::Greater(KeyString::from(bar)),
            "starts_with" => Test::Starts(KeyString::from(bar)),
            "ends_with" => Test::Ends(KeyString::from(bar)),
            "contains" => Test::Contains(KeyString::from(bar)),
            _ => todo!(),
        }
    }
}

pub enum ConditionBranch<'a> {
    Branch(Vec<&'a ConditionBranch<'a>>),
    Leaf(Condition),
}


/*
Alternative EZQL:

EZQL queries are written as functions calls with named parameters. The order of the parameters does not matter.

examples:   
INSERT(table_name: products, value_columns: (id, stock, location, price), new_values: ((0113035, 500, LAG15, 995), (0113000, 100, LAG30, 495)))
SELECT(primary_keys: *, table_name: products, conditions: ((price greater_than 500) AND (stock less_than 1000)))
UPDATE(table_name: products, primary_keys: (0113035, 0113000), conditions: ((id starts_with 011)), updates: ((price += 100), (stock -= 100)))
DELETE(primary_keys: *, table_name: products, conditions: ((price greater_than 500) AND (stock less_than 1000)))

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

    let first_paren = match query_string.find('(') {
        Some(x) => x,
        None => return Err(QueryError::InvalidQueryStructure("The arguments to the query must be surrounded by parentheses".to_owned()))
    };

    let mut query = Query::blank(&query_string[0..first_paren])?;

    let mut args: HashMap<String, Vec<String>> = HashMap::new();
    let mut current_arg = String::new();

    let mut escaped = false;
    for c in query_string.as_bytes()[first_paren..].iter() {
        if *c == b'\'' {
            escaped ^= true;
        }
        if escaped {
            state.word_buffer.push(*c);
            continue
        }
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
                        else {return Err(QueryError::InvalidQueryStructure("Parentheses do not match".to_owned()))}
                    }
                    None => return Err(QueryError::InvalidQueryStructure("Parentheses do not match".to_owned()))
                }
            },
            b':' => {
                let word = match String::from_utf8(state.word_buffer.clone()) {
                    Ok(s) => s.trim().to_owned(),
                    Err(e) => return Err(QueryError::InvalidQueryStructure(format!("Invalid utf8 encountered\nERROR TEXT: {e}"))),
                };
                if word.len() > 64 {
                    return Err(QueryError::TableNameTooLong)
                }
                current_arg = word;
                state.word_buffer.clear();
                
            },
            b',' => {
                let word = match String::from_utf8(state.word_buffer.clone()) {
                    Ok(s) => s.trim().to_owned(),
                    Err(e) => return Err(QueryError::InvalidQueryStructure(format!("Invalid utf8 encountered\nERROR TEXT: {e}"))),
                };
                state.word_buffer.clear();
                args.entry(current_arg.clone()).and_modify(|n| n.push(word.clone())).or_insert(vec![word.clone()]);
                
            },
            other => {
                state.word_buffer.push(*other);
            },         
        }
    }

    if !state.stack.is_empty() {
        return Err(QueryError::InvalidQueryStructure("Parentheses do not match".to_owned()))
    }

    let word = match String::from_utf8(state.word_buffer.clone()) {
        Ok(s) => s.trim().to_owned(),
        Err(e) => return Err(QueryError::InvalidQueryStructure(format!("Invalid utf8 encountered\nERROR TEXT: {e}"))),
    };
    state.word_buffer.clear();
    args.entry(current_arg.clone()).and_modify(|n| n.push(word.clone())).or_insert(vec![word.clone()]);

    match &mut query {
        Query::INSERT { table_name, inserts } => {
            let temp_table_name = match args.get("table_name") {
                Some(x) => {
                    let x = match x.first() {
                        Some(n) => n,
                        None => return Err(QueryError::InvalidQueryStructure("Missing table_name".to_owned())),
                    };
                    KeyString::from(x.as_str())
                },
                None => return Err(QueryError::InvalidQueryStructure("Missing table_name".to_owned())),
            };
            *table_name = KeyString::from(temp_table_name.as_str());

            let value_columns: Vec<KeyString> = match args.get("value_columns") {
                Some(x) => x.iter().map(|n| KeyString::from(n.as_str())).collect(),
                None => return Err(QueryError::InvalidQueryStructure("Missing value_columns".to_owned())),
            };
            let new_values = match args.get("new_values") {
                Some(x) => x,
                None => return Err(QueryError::InvalidQueryStructure("Missing new_values".to_owned())),
            };
            
            if new_values.len() % value_columns.len() != 0 {
                return Err(QueryError::InvalidQueryStructure("Number of values does not match number of columns".to_owned()));
            } else {
                let mut acc = String::with_capacity(2*new_values.len()*new_values[0].len());
                for tuple in new_values.chunks(value_columns.len()) {
                    for value in tuple {
                        acc.push_str(value);
                        acc.push(';');
                    }
                    acc.pop();
                    acc.push('\n');
                }
                acc.pop();
                *inserts = Inserts{value_columns: value_columns, new_values: acc};
            }

        },
        Query::SELECT { table_name, primary_keys, columns, conditions } => {

            (*table_name, *conditions, *primary_keys) = fill_fields(&args)?;
    
            match args.get("columns") {
                Some(x) => *columns = x.iter().map(|n| KeyString::from(n.as_str())).collect(),
                None => return Err(QueryError::InvalidQueryStructure("Missing column list. To select all columns use * as the columns argument.".to_owned())),
            };
        },

        Query::UPDATE { table_name, primary_keys, conditions, updates } => {
            (*table_name, *conditions, *primary_keys) = fill_fields(&args)?;

            let temp_updates = match args.get("updates") {
                Some(x) => x,
                None => return Err(QueryError::InvalidQueryStructure("Missing updates".to_owned())),
            };
            let mut acc = Vec::with_capacity(updates.len());
            for update in temp_updates {
                acc.push(Update::from_str(update)?);
            }
            *updates = acc;

        },

        Query::DELETE { primary_keys, table_name, conditions } => {
            (*table_name, *conditions, *primary_keys) = fill_fields(&args)?;
        },

        Query::LEFT_JOIN{ left_table_name: left_table, right_table_name: right_table, match_columns, primary_keys } => {

            let temp_left_table_name = match args.get("left_table") {
                Some(x) => match x.first() {
                    Some(n) => KeyString::from(n.as_str()),
                    None => return Err(QueryError::InvalidQueryStructure("Missing argument for left_table".to_owned())),
                },
                None => return Err(QueryError::InvalidQueryStructure("Missing left_table".to_owned())),
            };
            *left_table = temp_left_table_name;

            *right_table = match args.get("right_table") {
                Some(x) => match x.first() {
                    Some(n) => KeyString::from(n.as_str()),
                    None => return Err(QueryError::InvalidQueryStructure("Missing argument for right_table".to_owned())),
                },
                None => return Err(QueryError::InvalidQueryStructure("Missing right_table".to_owned())),
            };

            let temp_primary_keys = match args.get("primary_keys") {
                Some(x) => x,
                None => return Err(QueryError::InvalidQueryStructure("Missing primary_keys".to_owned())),
            };

            match temp_primary_keys.len() {
                0 => return Err(QueryError::InvalidQueryStructure("Missing argumenr for primary_keys".to_owned())),
                1 => {
                    match temp_primary_keys[0].as_str() {
                        "*" => *primary_keys = RangeOrListOrAll::All,
                        x => {
                            let mut split = x.split("..");
                            let start = match split.next() {
                                Some(x) => KeyString::from(x),
                                None => return Err(QueryError::InvalidQueryStructure("Ranges must have start and stop values".to_owned())),
                            };
                            let stop = match split.next() {
                                Some(x) => KeyString::from(x),
                                None => return Err(QueryError::InvalidQueryStructure("Ranges must have start and stop values".to_owned())),
                            };
                            *primary_keys = RangeOrListOrAll::Range(start, stop);
                        }
                    }
                },
                _ => {
                    let temp_primary_keys: Vec<KeyString> = temp_primary_keys.iter().map(|n| KeyString::from(n.as_str())).collect();
                    *primary_keys = RangeOrListOrAll::List(temp_primary_keys);
                }
            };

            let temp_match_columns: Vec<KeyString> = match args.get("match_columns") {
                Some(x) => x.iter().map(|s| KeyString::from(s.as_str())).collect(),
                None => return Err(QueryError::InvalidQueryStructure("Missing match_columns".to_owned())),
            };

            if temp_match_columns.len() != 2 {
                return Err(QueryError::InvalidQueryStructure("There should always be exactly two match columns, separated by a comma".to_owned()))
            } else {
                *match_columns = (temp_match_columns[0], temp_match_columns[1]);
            }
        },

        Query::SUMMARY{ table_name, columns } => {

            let temp_table_name = match args.get("table_name") {
                Some(x) => {
                    let x = match x.first() {
                        Some(n) => n,
                        None => return Err(QueryError::InvalidQueryStructure("Missing table_name".to_owned())),
                    };
                    KeyString::from(x.as_str())
                },
                None => return Err(QueryError::InvalidQueryStructure("Missing table_name".to_owned())),
            };
            *table_name = KeyString::from(temp_table_name.as_str());

            // SUMMARY(table_name: products, columns: ((SUM stock), (MEAN price)))
            let summary = match args.get("columns") {
                Some(x) => x,
                None => return Err(QueryError::InvalidQueryStructure("Missing columns".to_owned())),
            };

            let mut temp = Vec::with_capacity(summary.len());
            for stat in summary {
                let s = Statistic::from_str(stat)?;
                temp.push(s);
            }

            *columns = temp;

        },

        _ => unimplemented!()
    }


    Ok(query)

}

fn fill_fields(args: &HashMap<String, Vec<String>>) -> Result<(KeyString, Vec<OpOrCond>, RangeOrListOrAll), QueryError> {
    let table_name = match args.get("table_name") {
        Some(x) => {
            let x = match x.first() {
                Some(n) => n,
                None => return Err(QueryError::InvalidQueryStructure("Missing table_name".to_owned())),
            };
            KeyString::from(x.as_str())
        },
        None => return Err(QueryError::InvalidQueryStructure("Missing table_name".to_owned())),
    };
    let temp_conditions = match args.get("conditions") {
        Some(x) => {
            if x.len() != 1 {
                return Err(QueryError::InvalidQueryStructure("Conditions should be enclosed in parentheses and separated by whitespace".to_owned()))
            } else {
                x[0].split_whitespace().collect::<Vec<&str>>()
            }
        },
        None => return Err(QueryError::InvalidQueryStructure("Missing conditions. If you want no conditions just write 'conditions: ()'".to_owned())),
    };

    let mut condition_buffer = String::new();
    let mut conditions = Vec::new();
    for condition in temp_conditions {
        match condition {
            "AND" => {
                conditions.push(OpOrCond::Cond(Condition::from_str(condition_buffer.trim())?));
                condition_buffer.clear();
                conditions.push(OpOrCond::Op(Operator::AND));
            },
            "OR" => {
                conditions.push(OpOrCond::Cond(Condition::from_str(condition_buffer.trim())?));
                condition_buffer.clear();
                conditions.push(OpOrCond::Op(Operator::AND));
            },
            x => {
                condition_buffer.push_str(x);
                condition_buffer.push(' ');
            }
        }
    }
    if !condition_buffer.is_empty() {
        conditions.push(OpOrCond::Cond(Condition::from_str(condition_buffer.trim())?));
    }

    let temp_primary_keys = match args.get("primary_keys") {
        Some(x) => x,
        None => return Err(QueryError::InvalidQueryStructure("Missing primary_keys. To select all write: 'primary_keys: *'".to_owned())),
        };
        
    let primary_keys: RangeOrListOrAll;
    match temp_primary_keys.len() {
        0 => return Err(QueryError::InvalidQueryStructure("Missing argument for primary_keys".to_owned())),
        1 => {
            match temp_primary_keys[0].as_str() {
                "*" => primary_keys = RangeOrListOrAll::All,
                x => {
                    match x.find("..") {
                        Some(_) => {
                            let mut split = x.split("..");
                            let start = match split.next() {
                                Some(x) => KeyString::from(x),
                                None => return Err(QueryError::InvalidQueryStructure("Ranges must have start and stop values".to_owned())),
                            };
                            let stop = match split.next() {
                                Some(x) => KeyString::from(x),
                                None => return Err(QueryError::InvalidQueryStructure("Ranges must have both start and stop values".to_owned()))
                            };
                            primary_keys = RangeOrListOrAll::Range(start, stop);
                        },
                        None => {
                            primary_keys = RangeOrListOrAll::List(vec![KeyString::from(x)]);
                        }
                    }
                    
                }
            }
        },
        _ => {
            let temp_primary_keys: Vec<KeyString> = temp_primary_keys.iter().map(|n| KeyString::from(n.as_str())).collect();
            primary_keys = RangeOrListOrAll::List(temp_primary_keys);
        }
    };

    Ok((table_name, conditions, primary_keys))
}


pub fn subsplitter(s: &str) -> Vec<Vec<&str>> {

    let mut temp = Vec::new();
    for line in s.split(';') {
        temp.push(line.split(',').collect::<Vec<&str>>());
    }

    temp

}

pub fn is_even(x: usize) -> bool {
    0 == (x & 1)
}


pub fn parse_contained_token(s: &str, container_open: char, container_close: char) -> Option<&str> {
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

        match &query {
            Query::DELETE{ primary_keys: _, table_name, conditions: _ } => {
                match result_table {
                    Some(mut table) => result_table = execute_delete_query(query, &mut table)?,
                    None => {
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let mut table = tables.get(table_name).unwrap().write().unwrap();
                        result_table = execute_delete_query(query, &mut table)?;
                        database.buffer_pool.table_naughty_list.write().unwrap().insert(table.name);
                    },
                }
                
            },
            Query::SELECT{ table_name, primary_keys: _, columns: _, conditions: _ } => {
                match result_table {
                    Some(mut table) => result_table = execute_select_query(query, &mut table)?,
                    None => {
                        println!("table name: {}", table_name);
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let table = tables.get(table_name).unwrap().read().unwrap();
                        result_table = execute_select_query(query, &table)?;
                    },
                }
            },
            Query::LEFT_JOIN{ left_table_name, right_table_name, match_columns: _, primary_keys: _ } => {
                match result_table {
                    Some(table) => {
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let right_table = tables.get(right_table_name).unwrap().read().unwrap();
                        result_table = execute_left_join_query(query, &table, &right_table)?;
                    },
                    None => {
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let left_table = tables.get(left_table_name).unwrap().read().unwrap();
                        let right_table = tables.get(right_table_name).unwrap().read().unwrap();
                        execute_left_join_query(query, &left_table, &right_table)?;
                    },
                }
                
            },
            Query::INNER_JOIN => {
                unimplemented!("Inner joins are not yet implemented");
                // execute_inner_join_query(query, database);
            },
            Query::RIGHT_JOIN => {
                unimplemented!("Right joins are not yet implemented");

                // execute_right_join_query(query, database);
            },
            Query::FULL_JOIN => {
                unimplemented!("Full joins are not yet implemented");

                // execute_full_join_query(query, database);
            },
            Query::UPDATE{ table_name, primary_keys: _, conditions: _, updates: _ } => {
                match result_table {
                    Some(mut table) => result_table = execute_update_query(query, &mut table)?,
                    None => {
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let mut table = tables.get(table_name).unwrap().write().unwrap();
                        result_table = execute_update_query(query, &mut table)?;
                        database.buffer_pool.table_naughty_list.write().unwrap().insert(table.name);
                    },
                }
            },
            Query::INSERT{ table_name, inserts: _ } => {
                match result_table {
                    Some(mut table) => result_table = execute_insert_query(query, &mut table)?,
                    None => {
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let mut table = tables.get(table_name).unwrap().write().unwrap();
                        result_table = execute_insert_query(query, &mut table)?;
                        database.buffer_pool.table_naughty_list.write().unwrap().insert(table.name);
                    },
                }
            },
            Query::SUMMARY{ table_name, columns: _ } => {
                match result_table {
                    Some(table) => {
                        let result = execute_summary_query(query, &table)?;
                        match result {
                            Some(s) => return Ok(Some(s)),
                            None => todo!(),
                        };
                    },
                    None => {
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let table = tables.get(table_name).unwrap().read().unwrap();
                        let result = execute_summary_query(query, &table)?;
                        match result {
                            Some(s) => return Ok(Some(s)),
                            None => todo!(),
                        };
                    },
                }
            },
        }
    }

    match result_table {
        Some(table) => Ok(Some(table)),
        None => Ok(None),
    }
}


pub fn execute_delete_query(query: Query, table: &mut EZTable) -> Result<Option<EZTable>, ServerError> {
    
    match query {
        Query::DELETE { primary_keys, table_name: _, conditions } => {
            let keepers = filter_keepers(&conditions, &primary_keys, table)?;
            table.delete_by_indexes(&keepers);
        
            Ok(
                None
            )
        },
        other_query => return Err(ServerError::Query(format!("Wrong type of query passed to execute_delete_query() function.\nReceived query: {}", other_query))),
    }

}

pub fn execute_left_join_query(query: Query, left_table: &EZTable, right_table: &EZTable) -> Result<Option<EZTable>, ServerError> {
    
    match query {
        Query::LEFT_JOIN { left_table_name: _, right_table_name: _, match_columns, primary_keys } => {
            let filtered_indexes = keys_to_indexes(left_table, &primary_keys)?;
            let mut filtered_table = left_table.subtable_from_indexes(&filtered_indexes, &KeyString::from("__RESULT__"));
        
            filtered_table.alt_left_join(right_table, &match_columns.0)?;
        
            Ok(Some(filtered_table))
        },
        other_query => return Err(ServerError::Query(format!("Wrong type of query passed to execute_left_join_query() function.\nReceived query: {}", other_query))),
    }    
}

pub fn execute_update_query(query: Query, table: &mut EZTable) -> Result<Option<EZTable>, ServerError> {
    
    match query {
        Query::UPDATE { table_name: _, primary_keys, conditions, updates } => {
            let keepers = filter_keepers(&conditions, &primary_keys, table)?;

            for keeper in &keepers {
                for update in &updates{
                    if !table.columns.contains_key(&update.attribute) {
                        return Err(ServerError::Query(format!("Table does not contain column {}", update.attribute)))
                    }
                    match update.operator {
                        UpdateOp::Assign => {
                            match table.columns.get_mut(&update.attribute).unwrap() {
                                DbColumn::Ints(ref mut column) => column[*keeper] = update.value.to_i32(),
                                DbColumn::Floats(ref mut column) => column[*keeper] = update.value.to_f32(),
                                DbColumn::Texts(ref mut column) => column[*keeper] = update.value,
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
                                DbColumn::Texts(ref mut column) => column[*keeper] = update.value,
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
                                    let mut new = update.value;
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
        },
        other_query => return Err(ServerError::Query(format!("Wrong type of query passed to execute_update_query() function.\nReceived query: {}", other_query))),
    }

    
}

pub fn execute_insert_query(query: Query, table: &mut EZTable) -> Result<Option<EZTable>, ServerError> {

    match query {
        Query::INSERT { table_name: _, inserts } => {
            table.insert(inserts)?;
        
            Ok(
                None
            )
        },
        other_query => return Err(ServerError::Query(format!("Wrong type of query passed to execute_insert_query() function.\nReceived query: {}", other_query))),

    }
}

pub fn execute_select_query(query: Query, table: &EZTable) -> Result<Option<EZTable>, ServerError> {

    match query {
        Query::SELECT { table_name: _, primary_keys, columns, conditions } => {
            let keepers = filter_keepers(&conditions, &primary_keys, table)?;
        
            Ok(
                Some(
                    table
                        .subtable_from_indexes(&keepers, &KeyString::from("RESULT"))
                        .subtable_from_columns(&columns, "RESULT")?
                    )
            )
        },
        other_query => return Err(ServerError::Query(format!("Wrong type of query passed to execute_select_query() function.\nReceived query: {}", other_query))),
    }
}

pub fn execute_summary_query(query: Query, table: &EZTable) -> Result<Option<EZTable>, ServerError> {

    match query {
        Query::SUMMARY { table_name: _, columns } => {
            let mut result = EZTable::blank(&Vec::new(), KeyString::from("RESULT"), "QUERY");

            for stat in columns {
                let _ = match stat {
                    Statistic::SUM(column) => {
                        let requested_column = match table.columns.get(&column) {
                            Some(c) => c,
                            None => return Err(ServerError::Query(format!("No column named {} in table {}", column, table.name)))
                        };
                        match requested_column {
                            DbColumn::Floats(col) => result.add_column(KeyString::from(format!("SUM_{}", column).as_str()), DbColumn::Floats(vec![sum_f32_slice(col)])),
                            DbColumn::Ints(col) => {
                                match sum_i32_slice(col) {
                                    Some(x) => result.add_column(KeyString::from(format!("SUM_{}", column).as_str()), DbColumn::Ints(vec![x])),
                                    None => result.add_column(KeyString::from(format!("SUM_{}", column).as_str()), DbColumn::Texts(vec![KeyString::from("Operation would have overflowed i32")])),
                                }
                            },
                            DbColumn::Texts(_col) => result.add_column(KeyString::from(format!("SUM_{}", column).as_str()), DbColumn::Texts(vec![KeyString::from("Can't SUM a text column")])),
                        }
                    },
                    Statistic::MEAN(column) => {
                        let requested_column = match table.columns.get(&column) {
                            Some(c) => c,
                            None => return Err(ServerError::Query(format!("No column named {} in table {}", column, table.name)))
                        };
                        match requested_column {
                            DbColumn::Floats(col) => result.add_column(KeyString::from(format!("MEAN_{}", column).as_str()), DbColumn::Floats(vec![mean_f32_slice(col)])),
                            DbColumn::Ints(col) => result.add_column(KeyString::from(format!("MEAN_{}", column).as_str()), DbColumn::Floats(vec![mean_i32_slice(col)])),
                            DbColumn::Texts(_col) => result.add_column(KeyString::from(format!("MEAN_{}", column).as_str()), DbColumn::Texts(vec![KeyString::from("Can't mean a text column")])),
                        }
                    },
                    Statistic::MEDIAN(column) => {
                        let requested_column = match table.columns.get(&column) {
                            Some(c) => c,
                            None => return Err(ServerError::Query(format!("No column named {} in table {}", column, table.name)))
                        };
                        match requested_column {
                            DbColumn::Floats(col) => result.add_column(KeyString::from(format!("MEDIAN_{}", column).as_str()), DbColumn::Floats(vec![median_f32_slice(col)])),
                            DbColumn::Ints(col) => result.add_column(KeyString::from(format!("MEDIAN_{}", column).as_str()), DbColumn::Floats(vec![median_i32_slice(col)])),
                            DbColumn::Texts(_col) => result.add_column(KeyString::from(format!("MEDIAN_{}", column).as_str()), DbColumn::Texts(vec![KeyString::from("Can't median a text column")])),
                        }
                    },
                    Statistic::MODE(column) => {
                        let requested_column = match table.columns.get(&column) {
                            Some(c) => c,
                            None => return Err(ServerError::Query(format!("No column named {} in table {}", column, table.name)))
                        };
                        match requested_column {
                            DbColumn::Floats(_col) => result.add_column(KeyString::from(format!("MODE_{}", column).as_str()), DbColumn::Texts(vec![KeyString::from("Can't mode a float slice")])),
                            DbColumn::Ints(col) => result.add_column(KeyString::from(format!("MODE_{}", column).as_str()), DbColumn::Ints(vec![mode_i32_slice(col)])),
                            DbColumn::Texts(col) => result.add_column(KeyString::from(format!("MODE_{}", column).as_str()), DbColumn::Texts(vec![mode_string_slice(col)])),
                        }
                    },
                    Statistic::STDEV(column) => {
                        let requested_column = match table.columns.get(&column) {
                            Some(c) => c,
                            None => return Err(ServerError::Query(format!("No column named {} in table {}", column, table.name)))
                        };
                        match requested_column {
                            DbColumn::Floats(col) => result.add_column(KeyString::from(format!("STDEV_{}", column).as_str()), DbColumn::Floats(vec![stdev_f32_slice(col)])),
                            DbColumn::Ints(col) => result.add_column(KeyString::from(format!("STDEV_{}", column).as_str()), DbColumn::Floats(vec![stdev_i32_slice(col)])),
                            DbColumn::Texts(_col) => result.add_column(KeyString::from(format!("STDEV_{}", column).as_str()), DbColumn::Texts(vec![KeyString::from("Can't stdev a text column")])),
                        }
                    },
                };

            }


            Ok(Some(result))
        },
        other_query => return Err(ServerError::Query(format!("Wrong type of query passed to execute_select_query() function.\nReceived query: {}", other_query))),
    }

    
}

pub fn execute_inner_join_query(query: Query, database: Arc<Database>) -> Result<Option<EZTable>, ServerError> {
    
    // let tables = database.buffer_pool.tables.read().unwrap();
    // let table = tables.get(&query.table).unwrap().read().unwrap();
    // let keepers = filter_keepers(&query, &table)?;

    Err(ServerError::Unimplemented("inner joins are not yet implemented".to_owned()))
}

pub fn execute_right_join_query(query: Query, database: Arc<Database>) -> Result<Option<EZTable>, ServerError> {
    // let tables = database.buffer_pool.tables.read().unwrap();
    // let table = tables.get(&query.table).unwrap().read().unwrap();
    // let keepers = filter_keepers(&query, &table)?;

    Err(ServerError::Unimplemented("right joins are not yet implemented".to_owned()))
}

pub fn execute_full_join_query(query: Query, database: Arc<Database>) -> Result<Option<EZTable>, ServerError> {
    // let tables = database.buffer_pool.tables.read().unwrap();
    // let table = tables.get(&query.table).unwrap().read().unwrap();
    // let keepers = filter_keepers(&query, &table)?;

    Err(ServerError::Unimplemented("full joins are not yet implemented".to_owned()))
}

pub fn keys_to_indexes(table: &EZTable, keys: &RangeOrListOrAll) -> Result<Vec<usize>, StrictError> {
    let mut indexes = Vec::new();

    match keys {
        RangeOrListOrAll::Range(ref start, ref stop) => {
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
                    let first = match column.binary_search(start) {
                        Ok(x) => x,
                        Err(x) => x,
                    };
                    let last = match column.binary_search(stop) {
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
        RangeOrListOrAll::List(ref keys) => {
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
        RangeOrListOrAll::All => indexes = (0..table.len()).collect(),
    };

    Ok(indexes)
}


pub fn filter_keepers(conditions: &Vec<OpOrCond>, primary_keys: &RangeOrListOrAll, table: &EZTable) -> Result<Vec<usize>, ServerError> {
    let indexes = keys_to_indexes(table, primary_keys)?;
    
    if conditions.is_empty() {
        return Ok(indexes);
    }
    let mut keepers = Vec::<usize>::new();
    let mut current_op = Operator::OR;
    for condition in conditions.iter() {
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
                                    _ => return Err(ServerError::Query("Can only filter by 'starts_with' on text values".to_owned())),
                                }
                            },
                            Test::Ends(bar) => {
                                match column {
                                    DbColumn::Texts(col) => if col[*index].as_str().ends_with(bar.as_str()) {keepers.push(*index)},
                                    _ => return Err(ServerError::Query("Can only filter by 'ends_with' on text values".to_owned())),
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
                                    _ => return Err(ServerError::Query("Can only filter by 'starts_with' on text values".to_owned())),
                                }
                            },
                            Test::Ends(bar) => {
                                match column {
                                    DbColumn::Texts(col) => if col[*keeper].as_str().ends_with(bar.as_str()) {losers.push(*keeper)},
                                    _ => return Err(ServerError::Query("Can only filter by 'ends_with' on text values".to_owned())),
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
    // SELECT(primary_keys: *, table_name: products, conditions: ((price greater_than 500) AND (stock less_than 1000)))
    // UPDATE(table_name: products, primary_keys: (0113035, 0113000), conditions: ((id starts_with 011)), updates: ((price += 100), (stock -= 100)))
    // DELETE(primary_keys: *, table_name: products, conditions: ((price greater_than 500) AND (stock less_than 1000)))
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
        let SELECT_query_string = "SELECT(table_name: products, primary_keys: (0113000, 0113034, 0113035, 0113500), columns: *, conditions: ((price less_than 500) AND (price greater_than 200) AND (location equals lag15)))";
        let UPDATE_query_string = "UPDATE(table_name: products, primary_keys: (0113035, 0113000), conditions: ((id starts_with 011)), updates: ((price += 100), (stock -= 100)))";
        let DELETE_query_string = "DELETE(table_name: products, primary_keys: *, conditions: ((price greater_than 500) AND (stock less_than 1000)))";
        let LEFT_JOIN_query_string = "LEFT_JOIN(left_table: products, right_table: warehouses, primary_keys: 0113000..18572054, match_columns: (location, id))";
        let SUMMARY_query_string = "SUMMARY(table_name: products, columns: ((SUM stock), (MEAN price)))";
        
        let INSERT_query = parse_EZQL(INSERT_query_string).unwrap();
        dbg!(INSERT_query);
        // let SELECT_query = parse_EZQL(SELECT_query_string).unwrap();
        // let UPDATE_query = parse_EZQL(UPDATE_query_string).unwrap();
        // let DELETE_query = parse_EZQL(DELETE_query_string).unwrap();
        // let LEFT_JOIN_query = parse_EZQL(LEFT_JOIN_query_string).unwrap();
        // let SUMMARY_query = parse_EZQL(SUMMARY_query_string).unwrap();

        // println!("{}", &INSERT_query);
        // println!("{}", INSERT_query_string);
        // println!();

        // println!("{}", &SELECT_query);
        // println!("{}", SELECT_query_string);
        // println!();

        // println!("{}", &UPDATE_query);
        // println!("{}", UPDATE_query_string);
        // println!();

        // println!("{}", &DELETE_query);
        // println!("{}", DELETE_query_string);
        // println!();

        // println!("{}", &LEFT_JOIN_query);
        // println!("{}", LEFT_JOIN_query_string);
        // println!();

        // println!("{}", &SUMMARY_query);
        // println!("{}", SUMMARY_query_string);
        // println!();
        

        // assert_eq!(INSERT_query.to_string(), INSERT_query_string);
        // assert_eq!(SELECT_query.to_string(), SELECT_query_string);
        // assert_eq!(DELETE_query.to_string(), DELETE_query_string);
        // assert_eq!(UPDATE_query.to_string(), UPDATE_query_string);
        // assert_eq!(LEFT_JOIN_query.to_string(), LEFT_JOIN_query_string);
        // assert_eq!(SUMMARY_query.to_string(), SUMMARY_query_string);
    }

    #[test]
    fn test_updates() {
        let query = "UPDATE(table_name: products, primary_keys: (0113035, 0113000), conditions: ((id starts_with 011)), updates: ((price += 100), (stock -= 100)))";
        
        let parsed = parse_serial_query(query).unwrap();

        println!("{}", parsed[0]);
    }

    #[test]
    fn test_SELECT() {
        let table_string = std::fs::read_to_string(format!("test_files{PATH_SEP}good_csv.txt")).unwrap();
        let table = EZTable::from_csv_string(&table_string, "good_csv", "test").unwrap();
        let query = "SELECT(primary_keys: *, columns: *, table_name: good_csv, conditions: ())";
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
        let query = "UPDATE(table_name: products, primary_keys: *, conditions: ((id starts_with 011)), updates: ((price += 100), (stock -= 100)))";
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
        
        let INSERT_query = "INSERT(table_name: test, value_columns: (vnr, heiti, magn, lager), new_values: ( (175, HAMMAR, 52, lag15), (173, HAMMAR, 51, lag20) ))";
        let parsed_insert_query = parse_EZQL(&INSERT_query).unwrap();
        let google_docs_csv = std::fs::read_to_string(format!("test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted.csv")).unwrap();
        let mut t = EZTable::from_csv_string(&google_docs_csv, "test", "test").unwrap();
    
        execute_insert_query(parsed_insert_query, &mut t).unwrap();

        println!("t: \n{}", t);

    }

    #[test]
    fn test_INSERT_Products_bug() {
        let products = std::fs::read_to_string(format!("test_files{PATH_SEP}Products.csv")).unwrap();
        let mut products_table = EZTable::from_csv_string(&products, "Products", "test").unwrap();
        println!("{}", products_table);
        let query = "INSERT(table_name: Products, value_columns: (id, name, description, price, picture), new_values: (1,coke,refreshing beverage,200,coke))";
        let parsed_query = parse_EZQL(query).unwrap();
        println!("{}", parsed_query);
        execute_insert_query(parsed_query, &mut products_table).unwrap();
        println!("and then:\n{}", products_table);
        println!("-------------");

    }

    #[test]
    fn test_DELETE() {
        let query = "DELETE(table_name: products, primary_keys: *, conditions: ((price greater_than 3000) AND (stock less_than 1000)))";
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