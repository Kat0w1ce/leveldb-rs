pub mod slice;
pub mod util;
mod tests {
    use crate::slice::Slice;
    #[test]
    fn testsss() {
        let slice = Slice::from("s: &String");
    }
}
