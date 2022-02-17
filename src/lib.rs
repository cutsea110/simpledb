pub mod buffer;
pub mod file;
pub mod log;
pub mod query;
pub mod record;
pub mod tx;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
