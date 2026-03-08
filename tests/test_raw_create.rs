use talon::create_talon;

#[tokio::test]
async fn test_create_talon_empty() {
    let path = "/tmp/test_create_talon_3";
    let _ = std::fs::remove_dir_all(path);
    match create_talon(path).await {
        Ok(_) => println!("create_talon Success!"),
        Err(e) => {
            println!("create_talon Failed! {:?}", e);
            panic!("create_talon Error!");
        }
    }
}
