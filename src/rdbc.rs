pub mod connectionadapter;
pub mod driveradapter;
pub mod embedded;
pub mod resultsetadapter;
pub mod resultsetmetadataadapter;
pub mod statementadapter;

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    struct Conn {
        tx: Arc<Mutex<i32>>,
    }
    impl Conn {
        pub fn new() -> Self {
            Self {
                tx: Arc::new(Mutex::new(0)),
            }
        }
        pub fn create<'a>(&'a mut self) -> State<'a> {
            State::new(self)
        }
        pub fn update_tx(&self) {
            *self.tx.lock().unwrap() += 1;
        }
        pub fn get_tx(&self) -> i32 {
            *self.tx.lock().unwrap()
        }
    }
    struct State<'a> {
        conn: &'a mut Conn,
    }
    impl<'a> State<'a> {
        pub fn new(conn: &'a mut Conn) -> Self {
            State { conn }
        }
        pub fn update(&self) {
            self.conn.update_tx();
            println!("Tx {}", self.conn.get_tx());
        }
        pub fn execute(&self, cmd: &str) {
            println!("Execute {}", cmd);
            self.update();
        }
    }

    #[test]
    fn exam() {
        let mut conn = Conn::new();
        println!("Tx {}", conn.get_tx());

        let stmt = conn.create();
        stmt.execute("create table");
        stmt.execute("create view");
        stmt.execute("insert student");
        stmt.execute("insert dept");
    }
}
