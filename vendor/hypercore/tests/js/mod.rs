use std::fs::{create_dir_all, remove_dir_all, remove_file};
use std::path::Path;
use std::process::Command;

pub fn cleanup() {
    if Path::new("tests/js/node_modules").exists() {
        remove_dir_all("tests/js/node_modules").expect("Unable to run rm to delete node_modules");
    }

    if Path::new("tests/js/work").exists() {
        remove_dir_all("tests/js/work").expect("Unable to run rm to delete work");
    }
    if Path::new("tests/js/package-lock.json").exists() {
        remove_file("tests/js/package-lock.json")
            .expect("Unable to run rm to delete package-lock.json");
    }
}

pub fn install() {
    let status = Command::new("npm")
        .current_dir("tests/js")
        .args(["install"])
        .status()
        .expect("Unable to run npm install");
    assert_eq!(
        Some(0),
        status.code(),
        "npm install did not run successfully. Do you have npm installed and a network connection?"
    );
}

pub fn prepare_test_set(test_set: &str) -> String {
    let path = format!("tests/js/work/{}", test_set);
    create_dir_all(&path).expect("Unable to create work directory");
    path
}

pub fn js_run_step(step: u8, test_set: &str) {
    let status = Command::new("npm")
        .current_dir("tests/js")
        .args(["run", "step", &step.to_string(), test_set])
        .status()
        .expect("Unable to run npm run");
    assert_eq!(
        Some(0),
        status.code(),
        "node step {} did not run successfully",
        step
    );
}
