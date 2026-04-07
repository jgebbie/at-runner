use at_runner_client::{run_sync, ATSession, Step};

fn target() -> String {
    std::env::var("AT_RUNNER_TARGET").unwrap_or_else(|_| "localhost:50051".to_string())
}

const PEKERIS_ENV: &[u8] = b"\
'Pekeris problem'
50.0
1
'NVF'
0 0.0
100 1500.0 0.0 1.0 0.0 0.0 /
'A' 0.0
200.0 1600.0 0.0 1.5 0.5 0.0 /
1
1000.0 /
1
100.0 /
";

const PEKERIS_FLP: &[u8] = b"\
'Pekeris problem'
'RA'
9999
1
50.0 /
101
0.0 200.0 /
201
0.0 10000.0 /
";

// --- Tier 1: RunSync ---

#[test]
fn tier1_kraken_pekeris() {
    let result = run_sync(
        &target(),
        "kraken",
        "pekeris",
        &[("pekeris.env", PEKERIS_ENV)],
    )
    .expect("run_sync failed");

    assert_eq!(result.status, "completed");
    assert_eq!(result.exit_code, 0);
    assert!(result.files.contains_key("pekeris.prt"));
    assert!(result.files.contains_key("pekeris.mod"));
}

// --- Tier 2: Run with session ---

#[tokio::test]
async fn tier2_kraken_then_field() {
    let mut session = ATSession::connect(&target()).await.expect("connect failed");

    session
        .upload("pekeris.env", PEKERIS_ENV)
        .await
        .expect("upload env failed");

    let files = session.list_files().await.expect("list failed");
    assert!(files.iter().any(|(n, _)| n == "pekeris.env"));

    let result = session
        .run("kraken", "pekeris")
        .await
        .expect("run kraken failed");
    assert_eq!(result.status, "completed");
    assert_eq!(result.exit_code, 0);
    assert!(result.files.contains_key("pekeris.prt"));
    assert!(result.files.contains_key("pekeris.mod"));

    session
        .upload("pekeris.flp", PEKERIS_FLP)
        .await
        .expect("upload flp failed");

    let result2 = session
        .run("field", "pekeris")
        .await
        .expect("run field failed");
    assert_eq!(result2.status, "completed");
    assert_eq!(result2.exit_code, 0);
    assert!(result2.files.contains_key("pekeris.shd"));
}

#[tokio::test]
async fn tier2_upload_download_delete() {
    let mut session = ATSession::connect(&target()).await.expect("connect failed");

    let data = b"test content 12345";
    session
        .upload("test.txt", data)
        .await
        .expect("upload failed");

    let downloaded = session.download("test.txt").await.expect("download failed");
    assert_eq!(downloaded, data);

    session.delete("test.txt").await.expect("delete failed");

    let files = session.list_files().await.expect("list failed");
    assert!(!files.iter().any(|(n, _)| n == "test.txt"));
}

// --- Tier 3: RunPipeline ---

#[tokio::test]
async fn tier3_kraken_then_field_pipeline() {
    let mut session = ATSession::connect(&target()).await.expect("connect failed");

    let result = session
        .run_pipeline(&[
            Step::new("k1", "kraken", "pekeris").with_input("pekeris.env", PEKERIS_ENV),
            Step::new("f1", "field", "pekeris")
                .with_input("pekeris.flp", PEKERIS_FLP)
                .depends_on(&["k1"]),
        ])
        .await
        .expect("pipeline failed");

    assert!(result.all_succeeded);
    assert!(result.steps.contains_key("k1"));
    assert!(result.steps.contains_key("f1"));
    assert_eq!(result.steps["k1"].exit_code, 0);
    assert_eq!(result.steps["f1"].exit_code, 0);
    assert!(result.steps["k1"].files.contains_key("pekeris.mod"));
    assert!(result.steps["f1"].files.contains_key("pekeris.shd"));
}
