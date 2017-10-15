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

use std::collections::HashMap;
use std::iter::FromIterator;
use std::sync::Arc;

use hab_net::{self, time};
use hab_net::conn::{self, ConnEvent, ConnErr};
use hab_net::socket::DEFAULT_CONTEXT;
use protocol::{self, jobsrv, message};
use zmq;

use config::Config;
use error::{Error, Result};
use feat;
use log_forwarder::LogForwarder;
use runner::{RunnerCli, RunnerMgr};

/// Coordination signals for the Workers's main thread.
enum RecvEvent {
    /// Signals which sockets have pending messages to be processed.
    ///
    /// The containing tuple consists of 3 elements marking which sockets have pending messages.
    ///     * `0` - Incoming message from JobSrv
    ///     * `1` - Incoming message from Runner
    OnMessage((bool, bool)),
    /// Signals that the server is shutting down.
    Shutdown,
    /// Signals that no message events were received in the allotted time.
    Timeout,
}

enum State {
    Ready,
    Busy,
}

impl Default for State {
    fn default() -> State {
        State::Ready
    }
}

pub struct Server {
    config: Arc<Config>,
    /// Message buffer for server to RouterSrv Heartbeat.
    heartbeat: protocol::Message,
    /// Message buffer for reading complete protocol messages from Sockets.
    msg_buf: protocol::Message,
    net_ident: Arc<String>,
    /// Time in milliseconds when to send a heartbeat to JobSrv.
    next_heartbeat: i64,
    queue_addr: String,
    /// Dealer Socket connected to JobSrv
    queue_sock: zmq::Socket,
    /// Internal message buffer used for proxying messages between Router and Dispatcher sockets.
    recv_buf: zmq::Message,
    runner_cli: RunnerCli,
    state: State,
}

impl Server {
    pub fn new(config: Config) -> Result<Self> {
        let net_ident = hab_net::socket::srv_ident();
        let queue_sock = (**DEFAULT_CONTEXT).as_mut().socket(zmq::ROUTER)?;
        queue_sock.set_router_mandatory(true)?;
        queue_sock.set_probe_router(true)?;
        queue_sock.set_identity(net_ident.as_bytes())?;
        let mut heartbeat = jobsrv::Heartbeat::new();
        heartbeat.set_os(worker_os());
        heartbeat.set_state(jobsrv::WorkerState::Ready);
        Ok(Server {
            heartbeat: protocol::Message::build(&heartbeat).unwrap(),
            msg_buf: protocol::Message::default(),
            net_ident: Arc::new(net_ident),
            next_heartbeat: next_heartbeat(),
            queue_addr: config.queue_addr(),
            queue_sock: queue_sock,
            recv_buf: zmq::Message::new()?,
            runner_cli: RunnerCli::new(),
            state: State::default(),
            config: Arc::new(config),
        })
    }

    pub fn run(&mut self) -> Result<()> {
        if self.config.auth_token.is_empty() {
            error!(
                "ERROR: No 'auth_token' config value specified which prevents the \
                   worker from download fetching signing keys."
            );
            return Err(Error::NoAuthTokenError);
        }
        self.enable_features_from_config();

        RunnerMgr::start(self.config.clone(), self.net_ident.clone())?;
        LogForwarder::start(&self.config)?;
        self.runner_cli.connect()?;
        println!("Connecting to job queue, {}", self.queue_addr);
        self.queue_sock.connect(&self.queue_addr)?;

        info!("builder-worker is ready to go.");
        loop {
            trace!("waiting for message");
            match self.wait_recv() {
                RecvEvent::OnMessage((queue, runner)) => {
                    trace!("received messages, queue={}, runner={}", queue, runner);
                    if runner {
                        trace!("OnMessage, runner");
                        {
                            let reply = self.runner_cli.recv_complete()?;
                            self.queue_sock.send(reply, 0)?;
                        }
                        self.set_state(State::Ready)?;
                    }
                    if queue {
                        trace!("OnMessage, queue");
                        match conn::socket_read(
                            &self.queue_sock,
                            &mut self.msg_buf,
                            &mut self.recv_buf,
                        ) {
                            Ok(ConnEvent::OnConnect) => {
                                if let Err(err) = self.handle_connect() {
                                    error!("handle-connect, {}", err);
                                }
                            }
                            Ok(ConnEvent::OnMessage) => {
                                if let Err(err) = self.handle_message() {
                                    error!("handle-message, {}", err);
                                }
                            }
                            Err(err) => break Err(Error::from(err)),
                        }
                    }
                }
                RecvEvent::Timeout => {
                    self.next_heartbeat = next_heartbeat();
                    trace!("recv timeout, sending heartbeat to {}", self.queue_addr);
                    match conn::send_to(
                        &self.queue_sock,
                        &self.heartbeat,
                        self.queue_addr.as_bytes(),
                    ) {
                        Ok(()) => (),
                        Err(ConnErr::HostUnreachable) => {
                            trace!("jobsrv queue went away, {:?}", self.queue_addr);
                        }
                        Err(err) => break Err(Error::from(err)),
                    }
                }
                RecvEvent::Shutdown => {
                    info!("received shutdown signal, shutting down...");
                    let disconnect = protocol::Message::build(&jobsrv::Disconnect::new())?;
                    trace!("sending disconnect to {:?}", self.queue_addr);
                    conn::send_to(&self.queue_sock, &disconnect, self.queue_addr.as_bytes())
                        .unwrap();
                    break Ok(());
                }
            }
        }
    }

    /// Handle incoming server connect messages.
    fn handle_connect(&mut self) -> Result<()> {
        debug!("handle-connect, {:?}", self.msg_buf.sender_str().unwrap());
        conn::send_to(
            &self.queue_sock,
            &self.heartbeat,
            self.msg_buf.sender().unwrap(),
        ).map_err(Error::ConnErr)
    }

    /// Handle incoming protocol messages.
    ///
    /// Messages tagged with the `RouteSrv` protocol will be handled by the application itself
    /// while all other messages are handled by the `DispatcherPool`.
    fn handle_message(&mut self) -> Result<()> {
        debug!("handle-message, {:?}", self.msg_buf);
        let job = self.msg_buf.parse::<jobsrv::Job>().unwrap();
        match self.state {
            State::Ready => {
                self.runner_cli.send(&job)?;
                {
                    let reply = self.runner_cli.recv_ack()?;
                    self.queue_sock.send(reply, 0)?;
                }
                self.set_state(State::Busy)?;
            }
            State::Busy => {
                // JW TODO: handle unwrap
                let mut reply = self.msg_buf.parse::<jobsrv::Job>().unwrap();
                reply.set_state(jobsrv::JobState::Rejected);
                self.queue_sock.send(&message::encode(&reply)?, 0)?;
            }
        }
        Ok(())
    }

    fn set_state(&mut self, state: State) -> Result<()> {
        let mut heartbeat = self.heartbeat.parse::<jobsrv::Heartbeat>().unwrap();
        match state {
            State::Busy => heartbeat.set_state(jobsrv::WorkerState::Busy),
            State::Ready => heartbeat.set_state(jobsrv::WorkerState::Ready),
        }
        self.heartbeat = protocol::Message::build(&heartbeat).unwrap();
        self.state = state;
        // self.hb_cli.set_busy()?;
        Ok(())
    }

    fn enable_features_from_config(&self) {
        let features: HashMap<_, _> = HashMap::from_iter(vec![("LIST", feat::List)]);
        let features_enabled = self.config.features_enabled.split(",").map(|f| {
            f.trim().to_uppercase()
        });
        for key in features_enabled {
            if features.contains_key(key.as_str()) {
                info!("Enabling feature: {}", key);
                feat::enable(features.get(key.as_str()).unwrap().clone());
            }
        }

        if feat::is_enabled(feat::List) {
            println!("Listing possible feature flags: {:?}", features.keys());
            println!("Enable features by populating 'features_enabled' in config");
        }
    }

    /// Wait for incoming messages from RouteSrv(s) and Dispatchers and return a `RecvEvent` when
    /// a message is received, a timeout occurs, or the server is shutting down.
    fn wait_recv(&self) -> RecvEvent {
        let mut items = [
            self.queue_sock.as_poll_item(1),
            self.runner_cli.as_poll_item(1),
        ];
        match conn::socket_poll(&mut items, self.wait_timeout()) {
            Ok(count) => trace!("application received '{}' POLLIN events", count),
            Err(ConnErr::Timeout) => return RecvEvent::Timeout,
            Err(ConnErr::Shutdown(_)) => return RecvEvent::Shutdown,
            Err(err) => {
                error!("Error while waiting for socket events, {}", err);
                return RecvEvent::Shutdown;
            }
        }
        RecvEvent::OnMessage((items[0].is_readable(), items[1].is_readable()))
    }

    /// A tickless timer for determining how long to wait between each server tick. This value is
    /// variable depending upon when the next heartbeat is expected to occur.
    fn wait_timeout(&self) -> i64 {
        let time = self.next_heartbeat - time::clock_time();
        if time.is_negative() { 0 } else { time }
    }
}

pub fn run(config: Config) -> Result<()> {
    Server::new(config)?.run()
}

fn next_heartbeat() -> i64 {
    time::clock_time() + jobsrv::PING_INTERVAL_MS
}

#[cfg(target_os = "linux")]
fn worker_os() -> jobsrv::Os {
    jobsrv::Os::Linux
}

#[cfg(target_os = "windows")]
fn worker_os() -> jobsrv::Os {
    jobsrv::Os::Windows
}

#[cfg(target_os = "macos")]
fn worker_os() -> jobsrv::Os {
    jobsrv::Os::Darwin
}
