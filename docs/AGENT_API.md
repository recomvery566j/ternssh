# ternssh Agent API（程序化调用）

给 Agent 用的无状态 SSH 能力。**不走浏览器面板**，直接 HTTP。

状态：`POST /api/v1/agent/exec` 已上线并验证通过；文件传输（SFTP）开发中。

---

## 鉴权

`/api/*` 全部经过 `authenticateRequest()`，所以 `/api/v1/agent/*` **自动**受
Cloudflare Access 保护，**不需要另写鉴权代码**（这是它挂在 `/api` 下的真正原因，
不是任何 cookie 机制）。

Agent 用 **Access Service Token** 过 Zero Trust：

| 请求头 | 值 |
|---|---|
| `CF-Access-Client-Id` | Service Token 的 Client ID |
| `CF-Access-Client-Secret` | Service Token 的 Client Secret |

三个必须记住的坑（全部实测过）：

1. **Service Token 建好不会自动生效** —— 必须在 Access 应用上挂一条
   **Service Auth 策略**并包含该 token，否则请求仍然 302 跳登录页。
   干净的 A/B 证据：挂策略前 no-token 302 / token 也 302；挂上后
   no-token 302、**token 200 `{"ok":true}`**。
2. **必须带浏览器 User-Agent**。用 `Python-urllib` 之类的默认 UA，
   Cloudflare 边缘直接返回 `error code: 1010`，**根本到不了 Access**
   （早期两次"Access 基线"就是这么失败的——它测的其实是边缘拦截，不是鉴权）。
3. 错误路径会被 SPA 回退吞掉（`assets` binding +
   `not_found_handling: "single-page-application"`），返回 **HTML/405** 而不是 JSON 404。
   所以要判断"路由是否挂上"，只能看**正确路径能否返回 JSON**。

---

## POST /api/v1/agent/exec

请求：

```json
{ "node_id": "<D1 里的服务器 UUID>", "command": "uname -a", "timeout_ms": 15000 }
```

- `node_id` 必填，取自 `GET /api/v1/servers`（该端点返回 `{"tree":[...]}`）
- `command` 最长 8192 字符
- `timeout_ms` 可选，范围 1000–60000，默认 15000

响应 200：

```json
{ "success": true, "exit_code": 0, "stdout": "...", "stderr": "..." }
```

失败：

| 状态 | 含义 |
|---|---|
| 404 `{"error":"server not found"}` | `node_id` 不存在 |
| 504 | 命令执行超时 |
| 502 | 其他会话错误（`error` 字段给原因） |

**重试约定（安全性核心）**：只有"命令从未到达远端 shell"的错误才允许重试，
判据在 `isRetriableAgentError()` —— 仅匹配 `open failed` /
`Exec 通道打开失败` / `Exec 请求被拒绝` / `SSH 连接未就绪`。
**超时和 `已有命令正在执行` 刻意排除**，否则命令可能被执行两次。

同一个 DO 内命令**串行**执行（`agentExecChain`），实测并发请求会排队而不是交错。

---

## 退出码：一个已修的真 bug（留档，避免重犯）

**症状**：所有命令的 `exit_code` 都是 0（`exit 1` / `exit 42` / `false` 一律 0）。

**根因**：SSH 服务器发送顺序是 `exit-status` → `channel-eof` → `channel-close`，
而 `SSH_MSG_CHANNEL_EOF` / `SSH_MSG_CHANNEL_CLOSE` 分支**立即**用
`finalizeExec(0)` 结算。EOF 通常先于 `exit-status` 被处理，真退出码到达时
`pendingExec` 已被清空，于是**被静默丢弃**。

**为什么难查**：字节偏移、`readUint32` 大端实现、通道号匹配全都是对的
（`getChannelIDFromPayload` 与 `readUint32(payload, 1)` 等价，stdout 分流正常也
反证了解析无误）。问题不在解析，在**时序**。

**修法**：EOF/CLOSE 改为调用 `scheduleExecFallback(0, 300)` —— 给真退出码一个
300ms 窗口；`exit-status` 一到就立即结算并取消兜底定时器。正常情况下
**不引入额外延迟**（实测 `exit-status` 都在窗口内到达）。

**证据**（修复后 trace 里的稳定顺序）：

```
channel-eof  →  fallback scheduled in 300ms  →  exit-status code=1  →  settle exit=1  →  channel-close
```

---

## 部署（本项目最容易踩坑的地方）

生产 Worker：`ternssh.zhangxiaotian19990618.workers.dev`
部署方式：Cloudflare **Workers Builds**，连 GitHub `recomvery566j/ternssh`。

CF 构建配置有**三个**命令字段，语义**不可靠**：

| 字段 | 应填值 |
|---|---|
| 构建命令 | `npm run build` |
| 部署命令 | `npx wrangler deploy --config wrangler.production.jsonc` |
| 版本命令 | `npx wrangler deploy --config wrangler.production.jsonc` |

**两栏都要填真部署命令。** 实测它有时执行这栏、有时那栏：曾经把"部署命令"
填成 `echo skip`，结果**构建成功变绿、什么都没部署**（构建绿只是因为
`echo skip` 成功退出了）。

关键事实：

- `wrangler versions upload` **只创建版本，不切换生产流量**；
  只有 `wrangler deploy` 才真正部署。这是"构建成功但生产没更新"的根因。
- `wrangler.production.jsonc` 是 **gitignore** 的，由
  `scripts/generate-production-config.mjs` 在 postbuild 阶段从
  `wrangler.production.jsonc.example` 生成并填入真实 account / D1 ID。
- **push 触发构建不可靠**。实测 `38584f0` push 后自动构建了；
  `a615331` push 后等 2 分钟仍无新版本，需要去界面点**重试构建**。
- **判断是否真的部署了，别信页面**（页面可能是缓存的"41 分钟前"），查 API：
  - `GET /accounts/{A}/workers/scripts/ternssh/deployments`
  - `GET /accounts/{A}/workers/scripts/ternssh/versions`
  看最新 version id 是否变化。
- `/accounts/{A}/builds/workers/ternssh/builds` **返回空数组**，拿不到构建记录
  （它按 script tag 关联，与这个 Worker 对不上）；带短 id 的
  `/builds/builds/{id}/logs` 报 `Invalid uuid`。**别指望用 API 拉构建日志。**
- 界面版本列表里的"已手动部署"是**触发者/来源**标签，不是部署状态。

---

## 验证脚本

都在 `G:\win10数据\Desktop\dsh\tmp\`（随工作目录，不随仓库走）：

| 脚本 | 用途 |
|---|---|
| `cf_deploys.py` | 查 deployments + versions |
| `cf_versions.py` | 查 versions（含 alias / triggered_by） |
| `agent_probe_field.py` | 单次 exec，打印响应**所有**字段 —— 判断新代码是否上线 |
| `agent_exitcode2.py` | 退出码回归（7 个用例）+ 打印 exec_trace |

跑之前注意：

- **必须走代理**（`http://127.0.0.1:2090`，端口会变 —— 用
  `~/.dsh/skills/python-tooling/scripts/detect-proxy.py` 现探，别硬编码）
- 每个脚本开头 `sys.stdout.reconfigure(encoding="utf-8")`，否则中文静默乱码
- python 是 `python`，不是 `python3`

---

## 文件传输（SFTP）

三个端点，鉴权与 `node_id` 语义跟 `/exec` **完全一致**（同一个 DO、
同一份凭据解析、同样的 404/502/504 语义）。

### POST /api/v1/agent/sftp/list

```json
{ "node_id": "...", "path": "/etc" }
```

返回 `{ "success": true, "path": "/etc", "entries": [...] }`，
每个 entry 含 `name / type / size / sizeFormatted / permissions /
modifiedTime / isDir / isLink`。

### POST /api/v1/agent/sftp/read

```json
{ "node_id": "...", "path": "/etc/hosts", "encoding": "utf8", "max_bytes": 4194304 }
```

返回 `{ "success": true, "path": "...", "size": 123, "content": "..." }`。

- `encoding`：`utf8`（默认，`content` 是文本）或 `base64`（二进制安全）
- `max_bytes`：默认 4MB，硬上限 32MB。**超限直接报错，绝不静默截断**

### POST /api/v1/agent/sftp/write

```json
{ "node_id": "...", "path": "/tmp/x", "content": "hello", "encoding": "utf8" }
```

返回 `{ "success": true, "path": "...", "bytes_written": 5 }`。

- 会**创建或截断**目标文件
- 每个分块（32KB）都校验 SFTP 状态码，**写入失败不会被当成成功**

### 实现要点（给后来的维护者）

- 字节一律以 **base64** 穿过 JSON；`utf8` 只是路由层给调用方的糖，
  进了 DO 就只有一种格式
- 复用了浏览器面板那套 SFTP 通道机制，但给 `SFTPHandler` **另加**了三个
  直接返回结果的方法（`listEntriesDirect` / `readFileDirect` /
  `writeFileDirect`），**原有 UI 方法一行未改**
- agent 路径用一个**吞噬输出的假 WebSocket** 作为 `sftpConnections` 的键，
  因为那套管道是往 UI 推结果的
- **操作失败即丢弃 SFTP 通道**，下次调用重建 —— 半死的通道会把"一次可见的
  失败"变成"下一次的谜题"
- 这里查过一个**险些踩中的坑**：`resolveRemotePath()` 用的是 `realpath`，
  但它只对**父目录**做 realpath、文件名是拼上去的，所以上传新文件不会因为
  "文件不存在"而失败。**查了才知道**，否则会白写一层兜底
- [ ] 把临时的 `exec_trace` 字段换成布尔 `exit_code_verified`
      （告诉 Agent 退出码是真值还是 300ms 兜底值）
- [ ] 固化 skill，替换 `~/.dsh/AGENTS.md` 第 ④ 条的浏览器面板指引
- [ ] 凭据轮换报告

---

## 凭据位置（收尾必须报告并轮换）

- `C:\Users\recomvery\.dsh\cf-api-token.txt` —— CF API token
  （只读权限：Workers Builds/Scripts + Account Settings Read）
- `C:\Users\recomvery\.dsh\cf-access-service-token.env` —— Access Service Token
  （`CF_ACCESS_CLIENT_ID` / `CF_ACCESS_CLIENT_SECRET`）
- SSH 私钥 `~/.ssh/putty` **未泄露**（只读过首行 header 与派生公钥），**无需轮换**
