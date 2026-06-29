# CEO Agent Demo

这是基于 [callzhang/ceo-agent-service](https://github.com/callzhang/ceo-agent-service) 改造的一人 CEO 演示分支，用于验证 Friday 场景下“管理者分身 / Signal-to-Action Workbench”的产品形态。

原项目已经实现了钉钉消息读取、自动回复、任务抽取、审计记录、用户反馈、Memory Connector 和 Work Profile Distillation 等核心能力。完整能力说明、架构和原始使用方式请直接参考原项目：

- 原项目仓库：[callzhang/ceo-agent-service](https://github.com/callzhang/ceo-agent-service)
- 原项目文档入口：[README](https://github.com/callzhang/ceo-agent-service/blob/main/README.md)

本仓库不试图重写原项目能力说明，只记录为了 Friday 一人 CEO 演示做出的产品化改造和运行方式。

## 本仓库用途

- 作为 Friday 投资人演示中“一人 CEO 工作台”的可运行原型。
- 验证把 `ceo-agent-service` 作为 Friday Domain Pack 工作台能力底座的可行性。
- 演示从钉钉消息进入、Recipe / Memory 辅助判断、自动回复、审计记录、用户反馈回流的完整体验。
- 给后续 Friday Desktop / Friday Agent 研发对齐产品方案、交互和数据边界。

## 主要改动

### 工作台产品化

- 将原本偏开发者后台的审计控制台改造成更接近产品工作台的界面。
- 页面模块中文化，保留核心能力：处理记录、任务、用户反馈、设置、运行日志、执行会话。
- History 作为首页，支持卡片点击进入详情、hover 状态、滚动加载和详情页返回。
- Tasks 使用表格形式展示原项目字段，保留排序和全宽列表。
- Logs / Tasks / History 列表支持滚动加载，默认每页 100 条。
- 修复长消息、反馈链接和 URL 导致详情页横向溢出的问题。

### 钉钉连接与自动回复配置

- 设置页重新组织为“钉钉同步 + 自动回复策略 + Recipe / Memory 相关配置”的产品化结构。
- 增加钉钉连接状态展示，让用户能确认当前连接账号。
- 支持重新连接钉钉的交互入口。
- 保留原项目的可编辑配置项，包括：
  - `CEO_SINGLE_CHAT_ONLY`
  - `CEO_NOT_SEND_MESSAGE`
  - `CEO_DRY_RUN`
  - `CEO_ASSISTANT_SIGNATURE`
  - `CEO_HANDOFF_ACK`
  - `CEO_FEEDBACK_SPIKE_VERCEL_BASE_URL`
  - `CEO_PRODUCER_INTERVAL_SECONDS`
  - `FAST_PATH_UNREAD_BACKOFF`

### 用户反馈链路

- 回复发送后可自动附带 👍 / 👎 反馈链接。
- 新增外部反馈页，用户点击后可以快速提交“这条回复是否有帮助”。
- 支持从 URL 中读取 `rating=up/down` 并自动预选评分：
  - `up` -> “很有用”
  - `down` -> “不太有用”
- 反馈页使用 Friday logo 和更轻量的用户表单。
- 反馈数据支持写入 Vercel Blob 或 Tigris。
- 工作台“用户反馈”模块可按 feedback token 拉取外部反馈并展示。

### 响应速度与页面刷新

- 前端 History 页面由整页定时刷新改为接口轮询。
- 处理记录、任务、日志列表改为滚动加载。
- 下拉菜单改为自定义组件，避免系统默认样式和平台风格不一致。
- 全局主内容区限制最大宽度，详情页则按内容场景做适配。

### Friday 语境适配

- 设置页中以产品化方式呈现 Recipe、Memory、Connector 的关系。
- Recipe 在本场景中主要对应原项目的 developer prompt、规则、变量、回复边界和身份语气。
- Memory 对应原项目已经接入的 Memory MCP、工作画像、历史处理记录和反馈沉淀。
- Connector 对应钉钉连接、同步范围和自动回复策略。

## 和原项目的关系

原项目提供真实能力底座，本仓库主要做演示和产品化包装。

| 范围 | 来源 |
| --- | --- |
| 钉钉消息读取、发送、路由 | 原项目 |
| Codex Agent 结构化决策 | 原项目 |
| SQLite 审计记录 | 原项目 |
| Task 管理 | 原项目 |
| Memory Connector 接入 | 原项目 |
| Work Profile Distillation | 原项目 |
| 用户反馈模块基础能力 | 原项目 |
| Friday 风格工作台界面 | 本仓库改造 |
| 外部反馈页视觉和交互 | 本仓库改造 |
| 配置页产品化组织 | 本仓库改造 |
| 处理记录 / 任务 / 日志交互优化 | 本仓库改造 |

## 快速启动

### 安装依赖

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
npm install
```

### 配置环境变量

复制 `.env.example`：

```bash
cp .env.example .env
```

演示时常用配置：

```bash
CEO_SINGLE_CHAT_ONLY=1
CEO_NOT_SEND_MESSAGE=0
CEO_DRY_RUN=0
CEO_LIVE_SEND_BLOCKERS_ACCEPTED=1
CEO_FEEDBACK_SPIKE_VERCEL_BASE_URL=https://<your-feedback-app>.vercel.app
CEO_PRODUCER_INTERVAL_SECONDS=5
FAST_PATH_UNREAD_BACKOFF=1s
```

如果只想本地预演，不真实发送钉钉消息：

```bash
CEO_NOT_SEND_MESSAGE=1
CEO_DRY_RUN=1
```

不要提交 `.env`、SQLite 数据库、真实 workspace、钉钉导出材料、Codex session 或任何 token。

### 启动工作台

```bash
.venv/bin/python -m app.cli service \
  --host 127.0.0.1 \
  --port 8765 \
  --db ./data/auto-reply.sqlite3 \
  --workspace ./workspace \
  --corpus-dir ./data/corpus
```

打开：

```text
http://127.0.0.1:8765/
```

## 外部反馈页

本仓库包含一个可部署到 Vercel 的反馈页，用于钉钉回复后收集用户对该条回复的评价。

相关 API：

- `/api/dingtalk-feedback-spike`
- `/api/dingtalk-feedback-spike-events`

Vercel 环境变量至少需要配置一种存储后端。

### Vercel Blob

```text
BLOB_READ_WRITE_TOKEN=...
```

如果使用 Vercel Storage 的 Blob 连接，项目里通常还会有：

```text
BLOB_STORE_ID=...
BLOB_WEBHOOK_PUBLIC_KEY=...
```

### Tigris

```text
TIGRIS_STORAGE_ACCESS_KEY_ID=...
TIGRIS_STORAGE_SECRET_ACCESS_KEY=...
TIGRIS_STORAGE_BUCKET=...
```

如需全局读取反馈列表，可额外配置：

```text
FEEDBACK_SPIKE_SECRET=...
```

只按单条回复的 `feedback_token` 同步反馈时，不需要配置 `FEEDBACK_SPIKE_SECRET`。

## 目录结构

```text
.
├── app/                 # Python 服务、worker、审计 Web UI
├── api/                 # Vercel 反馈页和反馈事件 API
├── docs/                # 原项目文档和设计说明
├── launchd/             # macOS launchd 模板
├── scripts/             # 本地安装和运行脚本
├── tests/               # 测试
├── index.html           # 反馈页静态预览
├── friday-logo.svg      # Friday 反馈页 logo
├── package.json         # Vercel API 依赖
└── pyproject.toml       # Python 项目配置
```

## 测试

```bash
.venv/bin/pytest -q
```

只测试反馈页和反馈 API：

```bash
.venv/bin/pytest tests/test_feedback_page.py tests/test_feedback_spike_api.py -q
```

## 当前状态

- 已上传到个人仓库：[guanhao-fu-678/ceo-agent-demo](https://github.com/guanhao-fu-678/ceo-agent-demo)
- 当前分支：`main`
- 主要用于 Friday 一人 CEO 演示和产品方案对齐。

## License

MIT。原项目授权请参考 [callzhang/ceo-agent-service](https://github.com/callzhang/ceo-agent-service)。
