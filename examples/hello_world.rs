use kite_sql::db::{DataBaseBuilder, ResultIter};
use kite_sql::errors::DatabaseError;
use kite_sql::implement_from_tuple;
use kite_sql::types::value::DataValue;

#[derive(Default, Debug, PartialEq)]
struct MyStruct {
    pub c1: i32,
    pub c2: String,
}

implement_from_tuple!(
    MyStruct, (
        c1: i32 => |inner: &mut MyStruct, value| {
            if let DataValue::Int32(val) = value {
                inner.c1 = val;
            }
        },
        c2: String => |inner: &mut MyStruct, value| {
            if let DataValue::Utf8 { value, .. } = value {
                inner.c2 = value;
            }
        }
    )
);

#[cfg(feature = "macros")]
fn main() -> Result<(), DatabaseError> {
    let database = DataBaseBuilder::path("./hello_world").build()?;

    // database
    //     .run("CREATE TABLE t1 (id INT PRIMARY KEY, v1 VARCHAR(50), v2 INT)")?
    //     .done()?;
    // database
    //     .run("CREATE TABLE t2 (id INT PRIMARY KEY, v1 VARCHAR(50), v2 INT)")?
    //     .done()?;
    // database
    //     .run("CREATE TABLE t3 (id INT PRIMARY KEY, v1 INT, v2 INT)")?
    //     .done()?;
    // database
    //     .run("insert into t1(id, v1, v2) values (1,'a',9)")?
    //     .done()?;
    // database
    //     .run("insert into t1(id, v1, v2) values (2,'b',6)")?
    //     .done()?;
    // database
    //     .run("insert into t1(id, v1, v2) values (3,'c',11)")?
    //     .done()?;
    // database
    //     .run("insert into t2(id, v1, v2) values (1,'A',10)")?
    //     .done()?;
    // database
    //     .run("insert into t2(id, v1, v2) values (2,'B',11)")?
    //     .done()?;
    // database
    //     .run("insert into t3(id, v1, v2) values (1,6,10)")?
    //     .done()?;
    // database
    //     .run("insert into t2(id, v1, v2) values (3,'C',9)")?
    //     .done()?;
    // database
    //     .run("insert into t3(id, v1, v2) values (2,5,10)")?
    //     .done()?;
    // database
    //     .run("insert into t3(id, v1, v2) values (3,4,10)")?
    //     .done()?;

    let iter = database
        .run("SELECT id, v1, ( SELECT COUNT(*) FROM t2 WHERE t2.v2 >= 10 ) as cnt FROM t1")?;
    // let schema = iter.schema().clone();
    //
    for tuple in iter {
        println!("{:?}", tuple);
    }
    // database.run("drop table my_struct")?.done()?;
    //create table t(id int primary key, v1 int, v2 int, v3 int)
    Ok(())
}
