//! Language-server commands that are still executing, so frontends can list and cancel them.

use crate::Editor;
use lsp_client::{lsp, LanguageServerId};
use parking_lot::Mutex;
use std::{sync::Arc, time::Instant};
use tokio::task::AbortHandle;

/// A snapshot of one command executing on a language server.
#[derive(Debug, Clone)]
pub struct RunningLspCommand {
    pub id: u64,
    pub title: String,
    pub server_id: LanguageServerId,
    pub started: Instant,
}

#[derive(Default)]
struct State {
    next_id: u64,
    entries: Vec<(RunningLspCommand, AbortHandle)>,
}

/// Shared with each command task so it can deregister itself however it ends.
#[derive(Clone, Default)]
pub(crate) struct RunningLspCommands(Arc<Mutex<State>>);

/// Removes the registry entry when its task completes or is aborted.
struct Registration {
    commands: RunningLspCommands,
    id: u64,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.commands
            .0
            .lock()
            .entries
            .retain(|(c, _)| c.id != self.id);
    }
}

impl RunningLspCommands {
    /// Spawn `future` and track it until it finishes. Dropping the future on abort
    /// sends `$/cancelRequest` to the server.
    fn spawn(
        &self,
        title: String,
        server_id: LanguageServerId,
        future: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        // Hold the lock across spawn and insert so a fast task cannot deregister first.
        let mut state = self.0.lock();
        let id = state.next_id;
        state.next_id += 1;
        let registration = Registration {
            commands: self.clone(),
            id,
        };
        let task = tokio::spawn(async move {
            let _registration = registration;
            future.await;
        });
        let info = RunningLspCommand {
            id,
            title,
            server_id,
            started: Instant::now(),
        };
        state.entries.push((info, task.abort_handle()));
    }

    fn list(&self) -> Vec<RunningLspCommand> {
        self.0
            .lock()
            .entries
            .iter()
            .map(|(c, _)| c.clone())
            .collect()
    }

    fn cancel(&self, id: u64) -> bool {
        let handle = self
            .0
            .lock()
            .entries
            .iter()
            .find(|(c, _)| c.id == id)
            .map(|(_, handle)| handle.clone());
        handle.map(|handle| handle.abort()).is_some()
    }
}

impl Editor {
    /// Execute a command on a language server, tracking it until it completes.
    ///
    /// The command is executed on the server and communicated back to the client
    /// asynchronously using workspace edits.
    pub fn execute_lsp_command(&mut self, command: lsp::Command, server_id: LanguageServerId) {
        let title = command.title.clone();
        let Some(future) = self
            .language_server_by_id(server_id)
            .and_then(|server| server.command(command))
        else {
            self.set_error(|| "Language server does not support executing commands");
            return;
        };

        self.handlers
            .lsp_commands
            .spawn(title, server_id, async move {
                if let Err(err) = future.await {
                    log::error!("Error executing LSP command: {err}");
                }
            });
    }

    /// Language-server commands that have been sent and not yet answered, oldest first.
    pub fn running_lsp_commands(&self) -> Vec<RunningLspCommand> {
        self.handlers.lsp_commands.list()
    }

    /// Abort a running command and ask its server to stop work already sent.
    /// Returns false when it had already finished.
    pub fn cancel_lsp_command(&mut self, id: u64) -> bool {
        self.handlers.lsp_commands.cancel(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn settle() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    #[tokio::test]
    async fn cancelling_drops_the_future_and_deregisters() {
        let commands = RunningLspCommands::default();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel::<()>();
        commands.spawn("eval".into(), LanguageServerId::default(), async move {
            let _dropped = dropped_tx;
            std::future::pending::<()>().await;
        });
        let [running] = commands.list().try_into().unwrap();
        assert_eq!(running.title, "eval");

        assert!(commands.cancel(running.id));
        assert!(dropped_rx.await.is_err(), "future was dropped");
        settle().await;
        assert!(commands.list().is_empty());
        assert!(!commands.cancel(running.id));
    }

    #[tokio::test]
    async fn finished_commands_deregister() {
        let commands = RunningLspCommands::default();
        commands.spawn("quick".into(), LanguageServerId::default(), async {});
        settle().await;
        assert!(commands.list().is_empty());
    }
}
