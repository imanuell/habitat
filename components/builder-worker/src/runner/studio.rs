// Copyright (c) 2017 Chef Software Inc. and/or applicable contributors
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

use std::path::PathBuf;
use std::process::{Command, ExitStatus, Stdio};

use hab_core::channel::{BLDR_CHANNEL_ENVVAR, STABLE_CHANNEL};
use hab_core::env;
use hab_core::fs;
use hab_core::url::BLDR_URL_ENVVAR;

use error::Result;
use runner::log_pipe::LogPipe;
use runner::{NONINTERACTIVE_ENVVAR, RUNNER_DEBUG_ENVVAR};
use runner::workspace::Workspace;

lazy_static! {
    /// Absolute path to the Studio program
    static ref STUDIO_PROGRAM: PathBuf = fs::resolve_cmd_in_pkg(
        "hab-studio",
        include_str!(concat!(env!("OUT_DIR"), "/STUDIO_PKG_IDENT")),
    );
}

pub struct Studio<'a> {
    workspace: &'a Workspace,
    bldr_url: &'a str,
}

impl<'a> Studio<'a> {
    /// Creates a new Studio runner for a given `Workspace` and Builder URL.
    pub fn new(workspace: &'a Workspace, bldr_url: &'a str) -> Self {
        Studio {
            workspace,
            bldr_url,
        }
    }

    /// Spawns a Studio build command, pipes output streams to the given `LogPipe` and returns the
    /// process' `ExitStatus`.
    ///
    /// # Errors
    ///
    /// * If the child process can't be spawned
    /// * If the calling thread can't wait on the child process
    /// * If the `LogPipe` fails to pipe output
    pub fn build(&self, log_pipe: &mut LogPipe) -> Result<ExitStatus> {
        let channel = if self.workspace.job.has_channel() {
            self.workspace.job.get_channel()
        } else {
            STABLE_CHANNEL
        };

        let mut cmd = self.studio_command();
        cmd.arg("-k"); // Origin key
        cmd.arg(self.workspace.job.origin());
        cmd.arg("build");
        cmd.arg(build_path(self.workspace.job.get_project().get_plan_path()));
        debug!("building studio build command, cmd={:?}", &cmd);
        debug!(
            "setting studio build command env, {}={}",
            BLDR_CHANNEL_ENVVAR,
            &channel
        );
        cmd.env(BLDR_CHANNEL_ENVVAR, channel);
        debug!(
            "setting studio build command env, {}={}",
            BLDR_URL_ENVVAR,
            self.bldr_url
        );
        cmd.env(BLDR_URL_ENVVAR, self.bldr_url);

        debug!("spawning studio build command");
        let mut child = cmd.spawn()?;
        log_pipe.pipe(&mut child)?;
        let exit_status = child.wait()?;
        debug!("completed studio build command, status={:?}", exit_status);
        Ok(exit_status)
    }

    /// Spawns a Studio rm command and returns the process' `ExitStatus`.
    ///
    /// # Errors
    ///
    /// * If the child process can't be spawned
    /// * If the calling thread can't wait on the child process
    pub fn rm(&self) -> Result<ExitStatus> {
        let mut cmd = self.studio_command();
        cmd.arg("rm");
        debug!("building studio rm command, cmd={:?}", &cmd);

        debug!("spawning studio rm command");
        let mut child = cmd.spawn()?;
        let exit_status = child.wait()?;
        debug!("completed studio rm command, status={:?}", exit_status);
        Ok(exit_status)
    }

    /// Builds and returns a `Command` for spawning a Habitat Studio process.
    fn studio_command(&self) -> Command {
        let mut cmd = Command::new(&*STUDIO_PROGRAM);
        cmd.env_clear();
        if let Some(val) = env::var_os(RUNNER_DEBUG_ENVVAR) {
            cmd.env("DEBUG", val);
        }
        cmd.env(NONINTERACTIVE_ENVVAR, "true"); // Disables progress bars
        cmd.env("TERM", "xterm-256color"); // Emits ANSI color codes
        cmd.arg("-s"); // Source path
        cmd.arg(self.workspace.src());
        cmd.arg("-r"); // Studio root
        cmd.arg(self.workspace.studio());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        cmd
    }
}

/// Returns a path argument suitable to pass to a Studio build command.
fn build_path(plan_path: &str) -> String {
    let mut parts: Vec<_> = plan_path.split("/").collect();
    if parts.last().map_or("", |p| *p) == "plan.sh" {
        parts.pop();
    }
    if parts.last().map_or("", |p| *p) == "habitat" {
        parts.pop();
    }

    if parts.is_empty() {
        String::from(".")
    } else {
        parts.join("/")
    }
}

#[cfg(test)]
mod tests {
    use super::build_path;

    #[test]
    fn build_path_with_plan_sh() {
        assert_eq!(".", build_path("plan.sh"));
    }

    #[test]
    fn build_path_with_habitat_plan_sh() {
        assert_eq!(".", build_path("habitat/plan.sh"));
    }

    #[test]
    fn build_path_with_subdir_plan_sh() {
        assert_eq!("haaay", build_path("haaay/plan.sh"));
    }

    #[test]
    fn build_path_with_subdir_habitat_plan_sh() {
        assert_eq!(
            "components/yep",
            build_path("components/yep/habitat/plan.sh")
        );
    }
}
