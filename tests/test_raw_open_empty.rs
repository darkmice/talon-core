use talon::Talon;

#[test]
fn test_raw_open_empty() {
    let path = "/tmp/talon_test_empty";
    let _ = std::fs::remove_dir_all(path);
    match Talon::open(path) {
        Ok(_) => println!("Successfully opened empty Talon!"),
        Err(e) => {
            println!("Failed to open empty Talon: {:?}", e);
            panic!("Empty Talon error!");
        }
    }
}
