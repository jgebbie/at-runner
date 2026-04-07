use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use at_runner_client::{run_sync, ATSession, Step};

fn main() {
    let runners: Vec<String> = std::env::var("RUNNERS")
        .unwrap_or_else(|_| "localhost:50051".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    let tests_dir = PathBuf::from(
        std::env::var("TESTS_DIR").unwrap_or_else(|_| "/tests".to_string()),
    );

    let tests = discover_tests(&tests_dir);
    if tests.is_empty() {
        println!("No tests discovered");
        return;
    }

    println!("Discovered {} test cases", tests.len());
    for t in &tests {
        println!("  [{}] {}: {}", t.tier, t.name, t.models.join(", "));
    }

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    let mut results = Vec::new();
    for (i, test) in tests.iter().enumerate() {
        let runner = &runners[i % runners.len()];
        print!("  Running [{}] {}...", test.tier, test.name);

        let start = Instant::now();
        let status = match test.tier {
            1 => run_tier1(runner, test),
            2 => rt.block_on(run_tier2(runner, test)),
            3 => rt.block_on(run_tier3(runner, test)),
            _ => "skip".to_string(),
        };
        let elapsed = start.elapsed().as_secs_f64();
        println!(" {} ({:.1}s)", status, elapsed);
        results.push((test.name.clone(), test.tier, status, elapsed));
    }

    let passed = results.iter().filter(|r| r.2 == "pass").count();
    let failed = results.iter().filter(|r| r.2 == "fail").count();
    let errors = results.iter().filter(|r| r.2 == "error").count();

    println!("\n{}", "=".repeat(60));
    println!("Results: {} passed, {} failed, {} errors", passed, failed, errors);
    println!("{}", "=".repeat(60));

    if failed > 0 || errors > 0 {
        std::process::exit(1);
    }
}

struct TestCase {
    name: String,
    path: PathBuf,
    models: Vec<String>,
    tier: u8,
}

fn discover_tests(dir: &Path) -> Vec<TestCase> {
    let mut tests = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return tests;
    };

    let mut dirs: Vec<_> = entries.flatten().filter(|e| e.path().is_dir()).collect();
    dirs.sort_by_key(|e| e.file_name());

    for entry in dirs {
        let path = entry.path();
        let env_files: Vec<_> = fs::read_dir(&path)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "env")
                    .unwrap_or(false)
            })
            .collect();

        if env_files.is_empty() {
            continue;
        }

        let models = detect_models(&path);
        if models.is_empty() {
            continue;
        }

        let total_size: u64 = fs::read_dir(&path)
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| e.metadata().ok())
            .filter(|m| m.is_file())
            .map(|m| m.len())
            .sum();

        let tier = if models.len() > 1 {
            3
        } else if total_size > 1_000_000 {
            2
        } else {
            1
        };

        tests.push(TestCase {
            name: entry.file_name().to_string_lossy().to_string(),
            path,
            models,
            tier,
        });
    }

    tests
}

fn detect_models(test_dir: &Path) -> Vec<String> {
    let makefile = test_dir.join("Makefile");
    let known = [
        "bellhop", "bellhop3d", "kraken", "krakenc", "bounce", "field", "field3d", "scooter",
        "sparc",
    ];

    if let Ok(text) = fs::read_to_string(&makefile) {
        let lower = text.to_lowercase();
        let models: Vec<String> = known
            .iter()
            .filter(|m| lower.contains(&format!("{}.exe", m)) || lower.contains(&format!("{} ", m)))
            .map(|m| m.to_string())
            .collect();
        if !models.is_empty() {
            return models;
        }
    }

    let has_flp = fs::read_dir(test_dir)
        .into_iter()
        .flatten()
        .flatten()
        .any(|e| e.path().extension().map(|ext| ext == "flp").unwrap_or(false));

    if has_flp {
        vec!["kraken".to_string(), "field".to_string()]
    } else {
        vec!["kraken".to_string()]
    }
}

fn collect_inputs(test_dir: &Path) -> HashMap<String, Vec<u8>> {
    let mut inputs = HashMap::new();
    if let Ok(entries) = fs::read_dir(test_dir) {
        for entry in entries.flatten() {
            if entry.path().is_file() {
                if let Ok(data) = fs::read(entry.path()) {
                    inputs.insert(
                        entry.file_name().to_string_lossy().to_string(),
                        data,
                    );
                }
            }
        }
    }
    inputs
}

fn file_root(test_dir: &Path) -> String {
    fs::read_dir(test_dir)
        .into_iter()
        .flatten()
        .flatten()
        .find(|e| e.path().extension().map(|ext| ext == "env").unwrap_or(false))
        .map(|e| e.path().file_stem().unwrap().to_string_lossy().to_string())
        .unwrap_or_default()
}

fn run_tier1(runner: &str, test: &TestCase) -> String {
    let inputs = collect_inputs(&test.path);
    let root = file_root(&test.path);
    let input_refs: Vec<(&str, &[u8])> = inputs.iter().map(|(k, v)| (k.as_str(), v.as_slice())).collect();

    match run_sync(runner, &test.models[0], &root, &input_refs) {
        Ok(result) => {
            if result.exit_code == 0 {
                "pass".to_string()
            } else {
                "fail".to_string()
            }
        }
        Err(e) => {
            eprintln!("    error: {e}");
            "error".to_string()
        }
    }
}

async fn run_tier2(runner: &str, test: &TestCase) -> String {
    let inputs = collect_inputs(&test.path);
    let root = file_root(&test.path);

    let Ok(mut session) = ATSession::connect(runner).await else {
        return "error".to_string();
    };

    for (name, data) in &inputs {
        if session.upload(name, data).await.is_err() {
            return "error".to_string();
        }
    }

    match session.run(&test.models[0], &root).await {
        Ok(result) => {
            if result.exit_code == 0 {
                "pass".to_string()
            } else {
                "fail".to_string()
            }
        }
        Err(e) => {
            eprintln!("    error: {e}");
            "error".to_string()
        }
    }
}

async fn run_tier3(runner: &str, test: &TestCase) -> String {
    let inputs = collect_inputs(&test.path);
    let root = file_root(&test.path);

    let Ok(mut session) = ATSession::connect(runner).await else {
        return "error".to_string();
    };

    let mut steps = Vec::new();
    let mut prev_id: Option<String> = None;
    for (i, model) in test.models.iter().enumerate() {
        let step_id = format!("{}_{}", model, i);
        let step_inputs: Vec<(&str, &[u8])> = if i == 0 {
            inputs.iter().map(|(k, v)| (k.as_str(), v.as_slice())).collect()
        } else {
            Vec::new()
        };

        let mut step = Step::new(&step_id, model, &root);
        for (name, data) in step_inputs {
            step = step.with_input(name, data);
        }
        if let Some(ref dep) = prev_id {
            step = step.depends_on(&[dep.as_str()]);
        }
        prev_id = Some(step_id);
        steps.push(step);
    }

    match session.run_pipeline(&steps).await {
        Ok(result) => {
            if result.all_succeeded {
                "pass".to_string()
            } else {
                "fail".to_string()
            }
        }
        Err(e) => {
            eprintln!("    error: {e}");
            "error".to_string()
        }
    }
}
