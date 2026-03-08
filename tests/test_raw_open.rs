use talon::Talon;

#[test]
fn test_raw_open_local() {
    let path = "/Users/dark/.superclaw/talon_data";
    match Talon::open(path) {
        Ok(_) => println!("Successfully opened Talon!"),
        Err(e) => {
            println!("Failed to open Talon: {:?}", e);
            panic!("Talon error!");
        }
    }
}
