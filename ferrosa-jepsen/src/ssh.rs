use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use russh::client;
use russh::keys::key;
use russh::{ChannelMsg, Disconnect};
use tracing::{debug, info};

/// Output from a remote command execution.
#[derive(Debug, Clone)]
pub struct CommandOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

/// Minimal russh client handler that accepts all server keys.
///
/// In a Jepsen harness the VMs are ephemeral and we control the rootfs,
/// so strict host-key checking is unnecessary.
struct ClientHandler;

#[async_trait]
impl client::Handler for ClientHandler {
    type Error = anyhow::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &key::PublicKey,
    ) -> std::result::Result<bool, Self::Error> {
        // Accept all keys — VMs are ephemeral and we built the rootfs.
        Ok(true)
    }
}

/// SSH client wrapping a russh session handle.
///
/// Provides `exec` for running commands and `upload` for copying files
/// to the remote host.
pub struct SshClient {
    handle: client::Handle<ClientHandler>,
}

impl SshClient {
    /// Connect to a remote host via SSH using public-key authentication.
    ///
    /// `key_path` should point to an unencrypted private key (e.g., Ed25519).
    pub async fn connect(host: &str, port: u16, user: &str, key_path: &Path) -> Result<Self> {
        info!(host = %host, port = port, user = %user, "SSH connecting");

        let key_pair = russh_keys::load_secret_key(key_path, None)
            .with_context(|| format!("loading SSH key from {}", key_path.display()))?;

        let config = client::Config::default();

        let mut handle = client::connect(Arc::new(config), (host, port), ClientHandler)
            .await
            .context("SSH TCP connect")?;

        let auth_ok = handle
            .authenticate_publickey(user, Arc::new(key_pair))
            .await
            .context("SSH public-key authentication")?;

        if !auth_ok {
            bail!("SSH authentication rejected for user {user}");
        }

        info!("SSH connected and authenticated");
        Ok(Self { handle })
    }

    /// Execute a command on the remote host and collect its output.
    pub async fn exec(&self, command: &str) -> Result<CommandOutput> {
        debug!(command = %command, "SSH exec");

        let mut channel = self
            .handle
            .channel_open_session()
            .await
            .context("open SSH session channel")?;

        channel
            .exec(true, command)
            .await
            .context("send exec request")?;

        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut exit_code: Option<u32> = None;

        loop {
            match channel.wait().await {
                Some(ChannelMsg::Data { data }) => {
                    stdout.extend_from_slice(&data);
                }
                Some(ChannelMsg::ExtendedData { data, ext: 1 }) => {
                    // ext=1 is stderr per RFC 4254.
                    stderr.extend_from_slice(&data);
                }
                Some(ChannelMsg::ExitStatus { exit_status }) => {
                    exit_code = Some(exit_status);
                }
                Some(ChannelMsg::Eof | ChannelMsg::Close) => {
                    // The channel may send Eof then Close. Keep draining
                    // until the receiver yields None.
                }
                None => break,
                Some(_) => {
                    // Ignore other messages (WindowAdjusted, etc.).
                }
            }
        }

        let code = exit_code.unwrap_or(255) as i32;

        Ok(CommandOutput {
            stdout: String::from_utf8_lossy(&stdout).to_string(),
            stderr: String::from_utf8_lossy(&stderr).to_string(),
            exit_code: code,
        })
    }

    /// Upload a local file to a remote path using a shell-based approach.
    ///
    /// This uses `cat > remote_path` over an exec channel, which avoids
    /// requiring a full SFTP subsystem in the guest.
    pub async fn upload(&self, local_path: &Path, remote_path: &str) -> Result<()> {
        let contents = tokio::fs::read(local_path)
            .await
            .with_context(|| format!("reading local file {}", local_path.display()))?;

        debug!(
            local = %local_path.display(),
            remote = %remote_path,
            bytes = contents.len(),
            "SSH upload"
        );

        let mut channel = self
            .handle
            .channel_open_session()
            .await
            .context("open SSH session channel for upload")?;

        let cmd = format!("cat > '{remote_path}'");
        channel
            .exec(true, cmd.as_bytes())
            .await
            .context("send upload exec")?;

        channel
            .data(&contents[..])
            .await
            .context("send file data")?;

        channel.eof().await.context("send EOF after upload")?;

        // Drain until channel closes.
        let mut exit_code: Option<u32> = None;
        loop {
            match channel.wait().await {
                Some(ChannelMsg::ExitStatus { exit_status }) => {
                    exit_code = Some(exit_status);
                }
                Some(ChannelMsg::Eof | ChannelMsg::Close) => {}
                None => break,
                Some(_) => {}
            }
        }

        let code = exit_code.unwrap_or(255);
        if code != 0 {
            bail!("upload to {remote_path} failed with exit code {code}");
        }

        Ok(())
    }

    /// Disconnect the SSH session gracefully.
    pub async fn close(self) -> Result<()> {
        self.handle
            .disconnect(Disconnect::ByApplication, "", "en")
            .await
            .context("SSH disconnect")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_output_default_values() {
        let output = CommandOutput {
            stdout: "hello\n".to_string(),
            stderr: String::new(),
            exit_code: 0,
        };
        assert_eq!(output.exit_code, 0);
        assert_eq!(output.stdout.trim(), "hello");
    }

    /// Requires a pre-provisioned VM with SSH running and the test key.
    #[tokio::test]
    #[ignore]
    async fn ssh_execute_command() {
        let ssh = SshClient::connect("172.16.0.2", 22, "root", Path::new("rootfs/test_key"))
            .await
            .unwrap();

        let output = ssh.exec("echo hello").await.unwrap();
        assert_eq!(output.stdout.trim(), "hello");
        assert_eq!(output.exit_code, 0);
    }

    /// Requires a pre-provisioned VM with SSH running and the test key.
    #[tokio::test]
    #[ignore]
    async fn ssh_upload_file() {
        let ssh = SshClient::connect("172.16.0.2", 22, "root", Path::new("rootfs/test_key"))
            .await
            .unwrap();

        // Create a temporary file to upload.
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"test content").unwrap();

        ssh.upload(tmp.path(), "/tmp/uploaded_test").await.unwrap();

        let output = ssh.exec("cat /tmp/uploaded_test").await.unwrap();
        assert_eq!(output.stdout, "test content");
        assert_eq!(output.exit_code, 0);
    }
}
