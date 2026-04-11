use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;

#[derive(Debug, Clone)]
pub struct AppPaths {
    pub config_path: PathBuf,
    pub data_dir: PathBuf,
    pub tokens_path: PathBuf,
    pub state_path: PathBuf,
}

impl AppPaths {
    pub fn new(config_path: PathBuf, data_dir: PathBuf) -> Self {
        Self {
            config_path,
            tokens_path: data_dir.join("tokens.json"),
            state_path: data_dir.join("state.json"),
            data_dir,
        }
    }

    pub fn ensure_runtime_layout(&self) -> Result<()> {
        ensure_parent_dir(&self.config_path)?;
        fs::create_dir_all(&self.data_dir)?;
        Ok(())
    }
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }

    Ok(())
}
