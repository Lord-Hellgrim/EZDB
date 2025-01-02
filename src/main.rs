//#![allow(unused)]
//#![allow(non_snake_case)]


use EZDB::db_structure::ColumnTable;
use EZDB::db_structure::DbValue;
use EZDB::ezql::execute_select_query;
use EZDB::ezql::Condition;
use EZDB::ezql::OpOrCond;
use EZDB::ezql::Query;
use EZDB::ezql::RangeOrListOrAll;
use EZDB::ezql::TestOp;
use EZDB::server_networking;
use EZDB::utilities;

fn main() -> Result<(), utilities::EzError> {

    let massive_table_binary = std::fs::read("test_files/massive_table.eztable").unwrap();
        println!("HERE!");
        let massive_table = ColumnTable::from_binary("massive_table".into(), &massive_table_binary).unwrap();
        println!("HERE!");

        let query = Query::SELECT {
            table_name: "massive_table".into(),
            primary_keys: RangeOrListOrAll::All,
            columns: vec![
                "tqn[SNsonEhmBAbkTphVntSTPTqwyN]^EVnt".into(), 
                r"qlsCKiYAd_tko\PLNkoHwB`bUNlcTf_AryKdRKGmyo]ZixfsVNaELouL".into(),
                "oRMWqCfGSVjYydfSJeQnNgbPtqjQTaOTscYsxyy`NeeJVmU".into(),
            ],
            conditions: vec![
                OpOrCond::Cond(Condition{attribute: "tqn[SNsonEhmBAbkTphVntSTPTqwyN]^EVnt".into(), op: TestOp::Greater, value: DbValue::Float(0.0)}),
                OpOrCond::Cond(Condition{attribute: r"qlsCKiYAd_tko\PLNkoHwB`bUNlcTf_AryKdRKGmyo]ZixfsVNaELouL".into(), op: TestOp::Equals, value: DbValue::Text("Hella".into())}),
                OpOrCond::Cond(Condition{attribute: "oRMWqCfGSVjYydfSJeQnNgbPtqjQTaOTscYsxyy`NeeJVmU".into(), op: TestOp::Greater, value: DbValue::Int(0)}),
            ],
        };
        println!("HERE!");

        let start = std::time::Instant::now();
        execute_select_query(&query, &massive_table).unwrap().unwrap();
        let stop = start.elapsed().as_millis();
        println!("Time: {}ms", stop);

    println!("calling: main()");


    let args = std::env::args();

    for arg in args {
        println!("{}", arg);
    }

    // This stuff is for debugging purposes around simd
    #[cfg(target_feature="avx2")]
    unsafe fn p() {
        println!("AVX2");
    }

    // This stuff is for debugging purposes around simd
    #[cfg(not(target_feature="avx2"))]
    fn p() {
        println!("not avx2");
    }

    // This stuff is for debugging purposes around simd
    unsafe { p() };
    
    server_networking::run_server("127.0.0.1:3004")?;

    Ok(())
}
