// Helper macro to skip tests if TEST_INTEGRATION and TEST_KAFKA_ADDR
// environment variables are not set.
#[macro_export]
macro_rules! maybe_skip_integration {
    () => {{
        use std::env;

        match (
            env::var("TEST_INTEGRATION").is_ok(),
            env::var("TEST_KAFKA_ADDR").ok(),
        ) {
            (true, Some(addr)) => addr,
            (true, None) => {
                panic!(
                    "TEST_INTEGRATION is set which requires running integration tests, but TEST_KAFKA_ADDR is not set."
                )
            }
            (false, Some(_)) => {
                eprintln!("skipping end-to-end integration tests - set TEST_INTEGRATION to run");
                return;
            }
            (false, None) => {
                eprintln!(
                    "skipping end-to-end integration tests - set TEST_INTEGRATION and \
                    TEST_KAFKA_ADDR to run"
                );
                return;
            }
        }
    }}
}
