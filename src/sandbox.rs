//! 沙箱执行环境:统一后端抽象 + 两种实现。
//!
//! - **jail(默认)**:轻量目录监狱,零依赖。任务专属目录 + 路径强制锁定在沙箱根内 +
//!   Windows Job Object(内存/CPU/进程数限制、超时杀进程树)+ 命令白名单。
//!   防得住「模型误操作/乱跑命令」;不承诺防恶意逃逸(模型不是攻击者,此级别足够)。
//! - **docker**:最强隔离,`docker run --network none -m ... --cpus 1 -v root:/work`。
//!   需本机安装 Docker;命令无需白名单。
//!
//! 生命周期(沙箱模式任务专用):`setup` 搭建 → `activate` 激活 → 干活 → `destroy` 销毁。
//! 文件工具(读/写/列目录)由 agent.rs 调用 [`resolve_in_root`] 强制路径锁,两后端共用。

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::io::AsyncReadExt;

use crate::config::SandboxConfig;

/// 命令执行结果
#[derive(Debug, Clone)]
pub struct CmdOutput {
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

impl CmdOutput {
    pub fn summary(&self, cap: usize) -> String {
        let mut s = format!(
            "退出码: {}",
            self.code
                .map(|c| c.to_string())
                .unwrap_or_else(|| "被终止".into())
        );
        if !self.stdout.trim().is_empty() {
            s.push_str(&format!("\n[stdout] {}", truncate(&self.stdout, cap)));
        }
        if !self.stderr.trim().is_empty() {
            s.push_str(&format!("\n[stderr] {}", truncate(&self.stderr, cap)));
        }
        s
    }
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let t: String = s.chars().take(n).collect();
        format!("{t}…")
    }
}

/// 沙箱后端统一抽象(方法返回 boxed future 以保持 dyn 兼容)
pub trait SandboxBackend: Send + Sync {
    fn name(&self) -> &'static str;
    /// 沙箱根目录(所有文件工具与命令工作目录都限定在此)
    fn root(&self) -> &Path;
    /// 搭建沙箱(创建目录/准备环境;幂等,重复调用安全)
    fn setup<'a>(&'a self) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>>;
    /// 激活沙箱(搭建完成后、工具选择前调用;docker 后端会拉取镜像)
    fn activate<'a>(&'a self) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>>;
    /// 在沙箱内执行命令;workdir 必须已由调用方解析到沙箱根内
    fn run_cmd<'a>(
        &'a self,
        workdir: &'a Path,
        cmd: &'a str,
        args: &'a [String],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<CmdOutput, String>> + Send + 'a>>;
    /// 销毁沙箱(删除目录/容器)
    fn destroy(&self) -> Result<(), String>;
}

/// 后端工厂:按配置字符串选择实现("docker" → Docker 容器;其余 → 轻量目录监狱)
pub fn factory(
    backend: &str,
    cfg: &SandboxConfig,
    task_root: PathBuf,
) -> Result<Box<dyn SandboxBackend>, String> {
    match backend {
        "docker" => Ok(Box::new(DockerBackend::new(task_root, cfg.clone()))),
        _ => Ok(Box::new(JailBackend::new(task_root, cfg.clone()))),
    }
}

/// 路径锁定:把相对路径解析到沙箱根内。
/// 拒绝绝对路径、`..` 逃逸、盘符、UNC 路径。返回沙箱根内的绝对路径。
/// 以「规范化后的沙箱根」为基准拼接(根路径可能含符号链接/短名,避免比较失败)。
pub fn resolve_in_root(root: &Path, rel: &str) -> Result<PathBuf, String> {
    let rel = rel.trim().replace('\\', "/");
    if rel.is_empty() {
        return Err("路径不能为空".into());
    }
    if rel.starts_with('/') {
        return Err(format!("不允许绝对路径: {rel}"));
    }
    let mut parts = Vec::new();
    for comp in rel.split('/') {
        match comp {
            "" | "." => continue,
            ".." => return Err(format!("路径不允许包含 ..: {rel}")),
            c if c.contains(':') => return Err(format!("路径不允许包含盘符: {rel}")),
            c => parts.push(c),
        }
    }
    if parts.is_empty() {
        return Err("路径不能为空".into());
    }
    // 以规范化后的根为基准(根不存在时先创建)
    std::fs::create_dir_all(root).map_err(|e| format!("创建沙箱目录失败: {e}"))?;
    let root_canon = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let mut base = root_canon.clone();
    for p in parts {
        base.push(p);
    }
    // 规范化后再校验一次前缀(防组件拼接绕过)
    let canon = base.canonicalize().unwrap_or_else(|_| base.clone());
    if !canon.starts_with(&root_canon) {
        return Err(format!("路径越出沙箱: {rel}"));
    }
    Ok(canon)
}

/// 校验命令名在白名单内(仅 jail 后端;返回实际可执行文件名)。
/// Windows 下自动剥离 .exe 后缀后再比较(如 "python.exe" 匹配 "python")。
pub fn check_cmd_allowlist(allowlist: &str, cmd: &str) -> Result<String, String> {
    let cmd = cmd.trim();
    if cmd.is_empty() {
        return Err("命令不能为空".into());
    }
    let base = Path::new(cmd)
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| cmd.to_string());
    let normalized = base
        .trim_end_matches(".exe")
        .trim_end_matches(".EXE")
        .to_lowercase();
    let allowed: Vec<String> = allowlist
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    if allowed.is_empty() || allowed.iter().any(|a| *a == normalized) {
        Ok(base)
    } else {
        Err(format!("命令「{cmd}」不在白名单内(允许: {allowlist})"))
    }
}

// ---------- jail 后端 ----------

pub struct JailBackend {
    root: PathBuf,
    cfg: SandboxConfig,
}

impl JailBackend {
    pub fn new(root: PathBuf, cfg: SandboxConfig) -> Self {
        Self { root, cfg }
    }
}

impl SandboxBackend for JailBackend {
    fn name(&self) -> &'static str {
        "jail"
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn setup<'a>(&'a self) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            std::fs::create_dir_all(self.root.join("work"))
                .map_err(|e| format!("创建沙箱目录失败: {e}"))?;
            std::fs::create_dir_all(self.root.join("downloads"))
                .map_err(|e| format!("创建下载目录失败: {e}"))?;
            Ok(())
        })
    }

    fn activate<'a>(&'a self) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            // 目录监狱无需额外激活动作;存在即激活
            if !self.root.join("work").is_dir() {
                return Err("沙箱未搭建(work 目录缺失),请先执行 setup".into());
            }
            Ok(())
        })
    }

    fn run_cmd<'a>(
        &'a self,
        workdir: &'a Path,
        cmd: &'a str,
        args: &'a [String],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<CmdOutput, String>> + Send + 'a>> {
        Box::pin(async move {
            let base = check_cmd_allowlist(&self.cfg.cmd_allowlist, cmd)?;
            // 工作目录必须位于沙箱根内(防御:agent 层已解析,这里再查一次)
            let wd = workdir.canonicalize().unwrap_or_else(|_| workdir.to_path_buf());
            let root_canon = self.root.canonicalize().unwrap_or_else(|_| self.root.clone());
            if !wd.starts_with(&root_canon) {
                return Err(format!("工作目录越出沙箱: {}", workdir.display()));
            }
            let timeout = Duration::from_secs(self.cfg.cmd_timeout_secs.max(1));
            run_cmd_impl(&wd, &base, args, timeout, &self.cfg).await
        })
    }

    fn destroy(&self) -> Result<(), String> {
        match std::fs::remove_dir_all(&self.root) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(format!("清理沙箱目录失败: {e}")),
        }
    }
}

// ---------- docker 后端 ----------

pub struct DockerBackend {
    root: PathBuf,
    cfg: SandboxConfig,
}

impl DockerBackend {
    pub fn new(root: PathBuf, cfg: SandboxConfig) -> Self {
        Self { root, cfg }
    }

    fn image(&self) -> &str {
        let t = self.cfg.docker_image.trim();
        if t.is_empty() {
            "python:3.12-alpine"
        } else {
            t
        }
    }
}

impl SandboxBackend for DockerBackend {
    fn name(&self) -> &'static str {
        "docker"
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn setup<'a>(&'a self) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            std::fs::create_dir_all(self.root.join("work"))
                .map_err(|e| format!("创建沙箱目录失败: {e}"))?;
            std::fs::create_dir_all(self.root.join("downloads"))
                .map_err(|e| format!("创建下载目录失败: {e}"))?;
            // 探测 docker 可用性
            let args: Vec<String> = vec![
                "version".into(),
                "--format".into(),
                "{{.Server.Version}}".into(),
            ];
            let out = run_cmd_impl(&self.root, "docker", &args, Duration::from_secs(30), &self.cfg).await?;
            if out.code != Some(0) {
                return Err(format!("Docker 不可用: {}", out.summary(200)));
            }
            Ok(())
        })
    }

    fn activate<'a>(&'a self) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        let image = self.image().to_string();
        Box::pin(async move {
            // 拉取镜像(幂等;失败则任务终止)
            let args: Vec<String> = vec!["pull".into(), image];
            let out = run_cmd_impl(&self.root, "docker", &args, Duration::from_secs(600), &self.cfg).await?;
            if out.code != Some(0) {
                return Err(format!("拉取镜像失败: {}", out.summary(300)));
            }
            Ok(())
        })
    }

    fn run_cmd<'a>(
        &'a self,
        workdir: &'a Path,
        cmd: &'a str,
        args: &'a [String],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<CmdOutput, String>> + Send + 'a>> {
        let root = self.root.clone();
        let image = self.image().to_string();
        let mem = if self.cfg.docker_memory.trim().is_empty() {
            "512m".to_string()
        } else {
            self.cfg.docker_memory.trim().to_string()
        };
        let cmd = cmd.to_string();
        let args: Vec<String> = args.to_vec();
        let timeout = Duration::from_secs(self.cfg.cmd_timeout_secs.max(1));
        Box::pin(async move {
            if cmd.trim().is_empty() {
                return Err("命令不能为空".into());
            }
            // 容器内 /work 挂载沙箱根;workdir 相对沙箱根,转成容器内路径
            let rel = workdir
                .strip_prefix(&root)
                .unwrap_or(Path::new("work"));
            let container_wd = format!("/work/{}", rel.to_string_lossy().trim_start_matches('/'));
            let mut docker_args = vec![
                "run".to_string(),
                "--rm".to_string(),
                "--network".to_string(),
                "none".to_string(),
                "-m".to_string(),
                mem,
                "--cpus".to_string(),
                "1".to_string(),
                "-v".to_string(),
                format!("{}:/work", root.display()),
                "-w".to_string(),
                container_wd,
                image,
                cmd,
            ];
            docker_args.extend(args.iter().cloned());
            run_cmd_impl(&root, "docker", &docker_args, timeout, &self.cfg).await
        })
    }

    fn destroy(&self) -> Result<(), String> {
        // 容器 --rm 自动清理;删除挂载目录
        match std::fs::remove_dir_all(&self.root) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(format!("清理沙箱目录失败: {e}")),
        }
    }
}

// ---------- 命令执行公共实现 ----------

/// 通用命令执行:tokio 子进程 + 超时 + (Windows)Job Object 限制。
/// 注意:此函数仅内部使用,调用方负责白名单/工作目录校验。
async fn run_cmd_impl(
    workdir: &Path,
    cmd: &str,
    args: &[String],
    timeout: Duration,
    cfg: &SandboxConfig,
) -> Result<CmdOutput, String> {
    let mut command = tokio::process::Command::new(cmd);
    command
        .args(args)
        .current_dir(workdir)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .stdin(std::process::Stdio::null());

    #[cfg(windows)]
    {
        // tokio::process::Command 自带 creation_flags(CommandExt 已由其内部实现)
        // 不弹窗口;创建新进程组,便于终止
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        command.creation_flags(CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP);
    }

    // Windows:预创建 Job Object(内存/CPU/进程数限制 + KILL_ON_JOB_CLOSE 杀树)
    #[cfg(windows)]
    let job = JobGuard::new(cfg.mem_limit_mb, cfg.cpu_limit_secs);

    let mut child = command
        .spawn()
        .map_err(|e| format!("启动命令失败: {e}"))?;

    // Windows:把子进程加入 Job(tokio 的 id() 返回 Option)
    #[cfg(windows)]
    if let Some(j) = &job {
        if let Some(pid) = child.id() {
            if let Err(e) = j.assign(pid) {
                let _ = child.kill().await;
                return Err(e);
            }
        }
    }

    let mut stdout_pipe = child.stdout.take();
    let mut stderr_pipe = child.stderr.take();
    let started = std::time::Instant::now();

    let wait = tokio::time::timeout(timeout, child.wait()).await;
    let (code, timed_out) = match wait {
        Ok(Ok(st)) => (st.code(), false),
        Ok(Err(e)) => return Err(format!("等待命令失败: {e}")),
        Err(_) => {
            // 超时:先终止 Job 内全部进程(Windows),再补杀直接子进程(非 Windows)
            #[cfg(windows)]
            if let Some(j) = &job {
                j.kill();
            } else {
                let _ = child.kill().await;
            }
            #[cfg(not(windows))]
            let _ = child.kill().await;
            let _ = child.wait().await;
            (None, true)
        }
    };

    // 读取剩余输出(超时后进程已被杀,管道会关闭)
    let mut stdout = String::new();
    let mut stderr = String::new();
    if let Some(p) = &mut stdout_pipe {
        let _ = tokio::time::timeout(Duration::from_secs(5), p.read_to_string(&mut stdout)).await;
    }
    if let Some(p) = &mut stderr_pipe {
        let _ = tokio::time::timeout(Duration::from_secs(5), p.read_to_string(&mut stderr)).await;
    }

    if timed_out {
        Err(format!(
            "命令超时({}s > {}s),已终止",
            started.elapsed().as_secs(),
            timeout.as_secs()
        ))
    } else {
        Ok(CmdOutput {
            code,
            stdout,
            stderr,
        })
    }
}

/// Windows Job Object 守卫:限制内存/CPU/进程数,随 Drop 终止全部进程(KILL_ON_JOB_CLOSE)。
#[cfg(windows)]
pub struct JobGuard {
    handle: windows_sys::Win32::Foundation::HANDLE,
}

// HANDLE 是内核句柄,所有权保证任一时刻只有一个线程使用它(创建后即持有,不共享);
// JobGuard 自身可安全跨线程移动 —— 这使其能跨 await 存活(tokio future 需要 Send)。
#[cfg(windows)]
unsafe impl Send for JobGuard {}

#[cfg(windows)]
impl JobGuard {
    pub fn new(mem_limit_mb: u64, cpu_limit_secs: u64) -> Option<Self> {
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::JobObjects::*;
        unsafe {
            let handle = CreateJobObjectW(std::ptr::null(), std::ptr::null());
            if handle.is_null() {
                return None;
            }
            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
            let mut flags: u32 = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            if mem_limit_mb > 0 {
                flags |= JOB_OBJECT_LIMIT_JOB_MEMORY | JOB_OBJECT_LIMIT_PROCESS_MEMORY;
                info.JobMemoryLimit = (mem_limit_mb * 1024 * 1024) as usize;
                info.ProcessMemoryLimit = (mem_limit_mb * 1024 * 1024) as usize;
            }
            if cpu_limit_secs > 0 {
                flags |= JOB_OBJECT_LIMIT_JOB_TIME;
                info.BasicLimitInformation.PerJobUserTimeLimit =
                    (cpu_limit_secs as i64) * 10_000_000;
            }
            flags |= JOB_OBJECT_LIMIT_ACTIVE_PROCESS;
            info.BasicLimitInformation.ActiveProcessLimit = 16;
            info.BasicLimitInformation.LimitFlags = flags;
            let ok = SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                &info as *const _ as *const core::ffi::c_void,
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            );
            if ok == 0 {
                CloseHandle(handle);
                return None;
            }
            Some(Self { handle })
        }
    }

    pub fn assign(&self, pid: u32) -> Result<(), String> {
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::JobObjects::AssignProcessToJobObject;
        use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_SET_QUOTA, PROCESS_TERMINATE};
        unsafe {
            let ph = OpenProcess(PROCESS_SET_QUOTA | PROCESS_TERMINATE, 0, pid);
            if ph.is_null() {
                return Err(format!("无法打开进程 {pid} 加入沙箱 Job"));
            }
            let ok = AssignProcessToJobObject(self.handle, ph);
            let _ = CloseHandle(ph);
            if ok == 0 {
                return Err(format!("进程 {pid} 加入 Job 失败"));
            }
        }
        Ok(())
    }

    /// 终止 Job 内全部进程
    pub fn kill(&self) {
        use windows_sys::Win32::System::JobObjects::TerminateJobObject;
        unsafe {
            let _ = TerminateJobObject(self.handle, 1);
        }
    }
}

#[cfg(windows)]
impl Drop for JobGuard {
    fn drop(&mut self) {
        use windows_sys::Win32::Foundation::CloseHandle;
        unsafe {
            // KILL_ON_JOB_CLOSE:句柄关闭时终止 job 内所有进程
            let _ = CloseHandle(self.handle);
        }
    }
}

// ---------- 测试 ----------

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_root(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("lightbot_sbx_{name}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn resolve_path_locks_inside_root() {
        let root = tmp_root("path");
        std::fs::create_dir_all(&root).unwrap();
        let root_canon = root.canonicalize().unwrap_or_else(|_| root.clone());
        let p = resolve_in_root(&root, "a/b.txt").unwrap();
        assert!(p.starts_with(&root_canon), "p={} root={}", p.display(), root_canon.display());
        assert!(p.ends_with("b.txt"));
        // 逃逸拒绝
        assert!(resolve_in_root(&root, "../evil").is_err());
        assert!(resolve_in_root(&root, "C:/windows").is_err());
        assert!(resolve_in_root(&root, "/etc/passwd").is_err());
        assert!(resolve_in_root(&root, "a/../../evil").is_err());
        assert!(resolve_in_root(&root, "").is_err());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn allowlist_check() {
        let list = "python,py,node,pwsh";
        assert_eq!(check_cmd_allowlist(list, "python").unwrap(), "python");
        assert_eq!(check_cmd_allowlist(list, "python.exe").unwrap(), "python.exe");
        assert_eq!(check_cmd_allowlist(list, "pwsh").unwrap(), "pwsh");
        assert!(check_cmd_allowlist(list, "powershell").is_err());
        assert!(check_cmd_allowlist(list, "python3.12").is_err());
        assert!(check_cmd_allowlist(list, "cmd").is_err());
        assert!(check_cmd_allowlist(list, "").is_err());
        // 空白名单 = 不限制
        assert_eq!(check_cmd_allowlist("", "anything").unwrap(), "anything");
    }

    #[tokio::test]
    async fn jail_runs_cmd_and_timeout() {
        let root = tmp_root("cmd");
        // 收紧白名单(默认白名单含 powershell,测不到拦截)
        let mut cfg = SandboxConfig::default();
        cfg.cmd_allowlist = "python".into();
        let backend = JailBackend::new(root.clone(), cfg);
        backend.setup().await.unwrap();
        backend.activate().await.unwrap();

        let args: Vec<String> = vec!["-c".into(), "print('hello sandbox')".into()];
        let out = backend
            .run_cmd(&root.join("work"), "python", &args)
            .await
            .unwrap();
        assert_eq!(out.code, Some(0));
        assert!(out.stdout.contains("hello sandbox"));

        // 白名单拦截
        let args: Vec<String> = vec!["-Command".into(), "dir".into()];
        let err = backend
            .run_cmd(&root.join("work"), "powershell", &args)
            .await
            .unwrap_err();
        assert!(err.contains("白名单"));

        // 目录外拒绝
        let outside = tmp_root("outside");
        std::fs::create_dir_all(&outside).unwrap();
        let args: Vec<String> = vec!["-V".into()];
        let err = backend.run_cmd(&outside, "python", &args).await.unwrap_err();
        assert!(err.contains("越出沙箱"));

        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&outside);
    }

    #[tokio::test]
    async fn jail_timeout_kills() {
        let root = tmp_root("tm");
        let mut cfg = SandboxConfig::default();
        cfg.cmd_timeout_secs = 2;
        let backend = JailBackend::new(root.clone(), cfg);
        backend.setup().await.unwrap();
        let args: Vec<String> = vec!["-c".into(), "import time; time.sleep(30)".into()];
        let err = backend
            .run_cmd(&root.join("work"), "python", &args)
            .await
            .unwrap_err();
        assert!(err.contains("超时"), "err: {err}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn jail_destroy_removes_dir() {
        let root = tmp_root("destroy");
        let cfg = SandboxConfig::default();
        let backend = JailBackend::new(root.clone(), cfg);
        backend.setup().await.unwrap();
        assert!(root.exists());
        backend.destroy().unwrap();
        assert!(!root.exists());
        // 幂等
        backend.destroy().unwrap();
    }
}
