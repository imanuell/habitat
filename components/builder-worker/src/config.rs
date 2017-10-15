// Copyright (c) 2016 Chef Software Inc. and/or applicable contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Configuration for a Habitat JobSrv Worker

use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;

use github_api_client::config::GitHubCfg;
use hab_core::config::ConfigFile;
use hab_core::url;
use protocol::jobsrv::{DEFAULT_LOG_PORT, DEFAULT_WORKER_PORT};

use error::Error;

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Token for authenticating with the public builder-api
    pub auth_token: String,
    /// Enable automatic publishing for all builds by default
    pub auto_publish: bool,
    /// Filepath where persistent application data is stored
    pub data_path: PathBuf,
    /// Path to worker event logs
    pub log_path: PathBuf,
    /// Default channel name for Publish post-processor to use to determine which channel to
    /// publish artifacts to
    pub bldr_channel: String,
    /// Default URL for Publish post-processor to use to determine which Builder to use
    /// for retrieving signing keys and publishing artifacts
    pub bldr_url: String,
    /// List of Job Servers to connect to
    pub jobsrv: JobSrvCfg,
    pub features_enabled: String,
    /// Github application id to use for private repo access
    pub github: GitHubCfg,
}

impl Config {
    pub fn log_addr(&self) -> String {
        format!("tcp://{}:{}", self.jobsrv.host, self.jobsrv.log_port)
    }

    pub fn queue_addr(&self) -> String {
        format!("tcp://{}:{}", self.jobsrv.host, self.jobsrv.port)
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            auth_token: "".to_string(),
            auto_publish: true,
            data_path: PathBuf::from("/tmp"),
            log_path: PathBuf::from("/tmp"),
            bldr_channel: String::from("unstable"),
            bldr_url: url::default_bldr_url(),
            jobsrv: JobSrvCfg::default(),
            features_enabled: "".to_string(),
            github: GitHubCfg::default(),
        }
    }
}

impl ConfigFile for Config {
    type Error = Error;
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct JobSrvCfg {
    pub host: IpAddr,
    pub port: u16,
    pub log_port: u16,
}

impl Default for JobSrvCfg {
    fn default() -> Self {
        JobSrvCfg {
            host: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port: DEFAULT_WORKER_PORT,
            log_port: DEFAULT_LOG_PORT,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_from_file() {
        let content = r#"
        auth_token = "mytoken"
        data_path = "/path/to/data"
        log_path = "/path/to/logs"
        features_enabled = "FOO,BAR"

        [jobsrv]
        host = "2.2.2.2"
        port = 9000
        "#;

        let config = Config::from_raw(&content).unwrap();
        assert_eq!(&config.auth_token, "mytoken");
        assert_eq!(&format!("{}", config.data_path.display()), "/path/to/data");
        assert_eq!(&format!("{}", config.log_path.display()), "/path/to/logs");
        assert_eq!(&format!("{}", config.jobsrv.host), "1:1:1:1:1:1:1:1");
        assert_eq!(config.jobsrv.port, 9000);
        assert_eq!(config.jobsrv.log_port, 9021);
        assert_eq!(&config.features_enabled, "FOO,BAR");
    }
}
