use core::panic;
use helper_lib::fetch_var_from_env;
use postgres::types::ToSql;
use postgres::{Client, Error, NoTls, Row};
use std::collections::HashMap;

struct Author {
    id: i32,
    name: String,
    country: String,
}

struct Nation {
    nationality: String,
    count: i64,
}

fn main() -> Result<(), Error> {
    let db_url = fetch_var_from_env("DATABASE_URL");
    let mut client: Client = match Client::connect(&db_url, NoTls) {
        Ok(val) => val,
        Err(e) => panic!("Error in Db connecting : {}", e),
    };
    //Table creation Queries
    let author_def = "
    CREATE TABLE IF NOT EXISTS author (
        id              SERIAL PRIMARY KEY,
        name            VARCHAR NOT NULL,
        country         VARCHAR NOT NULL
        )
";
    let book_def = "
    CREATE TABLE IF NOT EXISTS book  (
        id              SERIAL PRIMARY KEY,
        title           VARCHAR NOT NULL,
        author_id       INTEGER NOT NULL REFERENCES author
        )
";
    let artist_def = "CREATE TABLE IF NOT EXISTS artist (
    id              SERIAL PRIMARY KEY,
    name          VARCHAR NOT NULL,
    book_name      VARCHAR NOT NULL,
    nationality      VARCHAR NOT NULL
    )
";

    let _: () = create_table_view(&mut client, author_def, "author");
    let _: () = create_table_view(&mut client, book_def, "book");
    let _: () = create_table_view(&mut client, artist_def, "artist");
    let affected_rows_of_author = execute_query(&mut client, "delete from author", &[], "author");
    // let affected_rows_of_artist=execute_query(&mut client, "delete from artist", &[],"author");
    let affected_rows_of_book = execute_query(&mut client, "delete from book", &[], "author");
    println!(
        "author {}  book{}",
        affected_rows_of_author, affected_rows_of_book
    );
    let mut authors = HashMap::new();
    authors.insert(String::from("Chinua Achebe"), "Nigeria");
    authors.insert(String::from("Rabindranath Tagore"), "India");
    authors.insert(String::from("Anita Nair"), "India");
    //Insert the data into author table
    let mut id = 0;
    for (key, value) in &authors {
        let author = Author {
            id: id,
            name: key.to_string(),
            country: value.to_string(),
        };
        id += 1;

        let _modified_rows = match client.execute(
            "INSERT INTO author (id,name, country) VALUES ($1, $2,$3)",
            &[&author.id, &author.name, &author.country],
        ) {
            Ok(val) => val,
            Err(e) => panic!("Data can not be inserted in author table Error:{}", e),
        };
        println!("total no. of modified rows: {}", _modified_rows);
    }

    //extracted the data from author table
    let _author_data = select_query(
        &mut client,
        "SELECT id, name, country FROM author",
        &[],
        "author",
    );
    for row in match client.query("SELECT id, name, country FROM author", &[]) {
        Ok(val) => val,
        Err(e) => panic!("Data can not be fetched from author table Error: {}", e),
    } {
        let author = Author {
            id: row.get(0),
            name: row.get(1),
            country: row.get(2),
        };
        println!("Author {} is from {}", author.name, author.country);
    }
    //extracted the data from artists table
    for row in match client.query(
        "SELECT nationality, COUNT(nationality) AS count 
    FROM artist GROUP BY nationality ORDER BY count DESC",
        &[],
    ) {
        Ok(val) => val,
        Err(e) => panic!("Data can not be fetched from artist table Error: {}", e),
    } {
        let (nationality, count): (Option<String>, Option<i64>) = (row.get(0), row.get(1));

        if nationality.is_some() && count.is_some() {
            let nation = Nation {
                nationality: nationality.unwrap(),
                count: count.unwrap(),
            };
            println!("{} {}", nation.nationality, nation.count);
        }
    }

    Ok(())
}
fn create_table_view(client_name: &mut Client, table_def: &str, table_name: &str) -> () {
    match client_name.batch_execute(table_def) {
        Ok(val) => val,
        Err(e) => panic!(
            "{} table can not be created due the error: {}",
            table_name, e
        ),
    };
}
fn execute_query(
    client: &mut Client,
    query: &str,
    params: &[&(dyn ToSql + Sync)],
    table_name: &str,
) -> u64 {
    let _modified_rows = match client.execute(query, &params) {
        Ok(val) => val,
        Err(e) => panic!(
            "Data can not be inserted in table {}. Error:{}",
            table_name, e
        ),
    };
    _modified_rows
}
fn select_query(
    client: &mut Client,
    query: &str,
    params: &[&(dyn ToSql + Sync)],
    table_name: &str,
) -> Vec<Row> {
    let modified_rows = match client.query(query, &params) {
        Ok(val) => val,
        Err(e) => panic!(
            "Data can not be inserted in table {}. Error:{}",
            table_name, e
        ),
    };
    modified_rows
}
