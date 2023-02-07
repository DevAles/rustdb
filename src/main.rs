use lazy_static::lazy_static;
use postgres::{Client, Error, NoTls};

const INIT_DB: &str = "
    CREATE TABLE IF NOT EXISTS Users (
        id      SERIAL PRIMARY KEY,
        name    TEXT NOT NULL,
        email   TEXT NOT NULL UNIQUE
    );
";

lazy_static! {
    static ref RESET_DB: String = format!(
        "DROP TABLE Users;
        {}
        ALTER SEQUENCE Users_id_seq RESTART WITH 1;
    ",
        INIT_DB,
    );
}

struct Db {
    client: Client,
}

impl Db {
    pub fn new() -> Result<Db, Error> {
        let mut client = Client::connect("host=localhost user=devales dbname=tests", NoTls)?;
        client.batch_execute(INIT_DB)?;

        Ok(Db { client })
    }

    pub fn insert(&mut self, name: &str, email: &str) -> Result<(), Error> {
        self.client.execute(
            "INSERT INTO Users (name, email) VALUES ($1, $2)",
            &[&name, &email],
        )?;

        Ok(())
    }

    pub fn select(&mut self, query: &str) -> Result<Vec<postgres::Row>, Error> {
        let statement = format!("SELECT {} FROM Users", query);

        Ok(self.client.query(&statement, &[])?)
    }

    pub fn update(
        &mut self,
        old_email: &str,
        new_name: &str,
        new_email: &str,
    ) -> Result<(), Error> {
        self.client.execute(
            "UPDATE Users SET name=$1, email=$2 WHERE email=$3",
            &[&new_name, &new_email, &old_email],
        )?;

        Ok(())
    }

    pub fn delete(&mut self, email: &str) -> Result<(), Error> {
        self.client
            .execute("DELETE FROM Users WHERE email=$1", &[&email])?;

        Ok(())
    }

    pub fn reset(&mut self) -> Result<(), Error> {
        self.client.batch_execute(&RESET_DB).unwrap();

        Ok(())
    }
}

fn main() {
    if let Err(e) = run() {
        println!("{}", e);
    }
}

fn run() -> Result<(), postgres::Error> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::Db;

    #[test]
    fn select() {
        let mut db = Db::new().unwrap();
        let rows = db.select("*").unwrap();

        assert_eq!(rows.len(), 0);
        db.reset().unwrap();
    }

    #[test]
    fn insert() {
        let mut db = Db::new().unwrap();
        let name = "Ferris";
        let email = "ferris@gmail.com";

        db.insert(name, email).unwrap();

        for row in db.select("id, name, email").unwrap() {
            let id: i32 = row.get(0);
            let name: &str = row.get(1);
            let email: &str = row.get(2);

            assert_eq!(id, 1);
            assert_eq!(name, "Ferris");
            assert_eq!(email, "ferris@gmail.com");
        }

        db.reset().unwrap();
    }

    #[test]
    fn update() {
        let mut db = Db::new().unwrap();
        let name = "Frris";
        let email = "frris@gmail.com";

        db.insert(name, email).unwrap();
        db.update(email, "Ferris", "ferris@gmail.com").unwrap();

        for row in db.select("id, name, email").unwrap() {
            let id: i32 = row.get(0);
            let name: &str = row.get(1);
            let email: &str = row.get(2);

            assert_eq!(id, 1);
            assert_eq!(name, "Ferris");
            assert_eq!(email, "ferris@gmail.com");
        }

        db.reset().unwrap();
    }

    #[test]
    fn delete() {
        let mut db = Db::new().unwrap();
        db.delete("*").unwrap();
        let rows = db.select("*").unwrap();

        assert_eq!(rows.len(), 0);
        db.reset().unwrap();
    }
}
