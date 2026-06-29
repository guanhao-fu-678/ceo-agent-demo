import json
import asyncio
import hashlib
from collections.abc import Callable, Iterable, Mapping
from collections import deque
from datetime import datetime, timedelta, timezone, tzinfo
from html import escape
from itertools import count, zip_longest
import os
from pathlib import Path
import subprocess
from typing import TypedDict
import urllib.error
import urllib.request
from urllib.parse import parse_qs, quote, urlencode, urlparse

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)

from app.codex_history import (
    RenderedCodexEvent,
    extract_codex_audit_events_from_session,
    render_local_codex_session,
)
from app.codex_decision import audit_summary_explains_no_documents
from app.config import (
    assistant_signature,
    batch_seconds,
    broadcast_mention_aliases,
    consumer_poll_interval_seconds,
    corpus_dir,
    document_extraction_ids,
    env_file_path,
    fast_path_unread_backoff_duration,
    feedback_spike_vercel_base_url,
    forbidden_path_prefixes,
    handoff_ack,
    memory_connector_user_id,
    mention_aliases,
    message_recovery_interval,
    poll_interval_seconds,
    principal_name,
    producer_interval_seconds,
    read_env_file,
    single_chat_read_recovery_limit,
    single_chat_read_recovery_window,
    single_chat_only,
    user_alias,
    worker_db_path,
    write_env_values,
    work_profile_path,
    workspace_path,
)
from app.developer_prompt import (
    configurable_prompt_variable_pairs,
    DeveloperPromptTemplateError,
    developer_prompt_template_path,
    prompt_variable_env_key,
    read_developer_prompt_template,
    read_user_prompt_template,
    render_developer_prompt_template,
    render_user_prompt_template,
    split_developer_prompt_template,
    user_prompt_template_path,
    write_developer_prompt_template,
    write_configurable_prompt_variables,
    write_user_prompt_template,
)
from app.memory_setup import codex_config_has_memory_connector
from app.dingtalk_models import (
    CodexAction,
    DingTalkConversation,
    DingTalkMessage,
    SensitivityKind,
)
from app.dws_client import DwsClient, DwsError, DwsUserProfile
from app.feedback_spike import (
    FeedbackLinkContext,
    extract_feedback_link_context,
)
from app.feedback_events import (
    feedback_context_for_sent_reply,
    sync_feedback_events_for_context as sync_feedback_events_for_context_impl,
    sync_feedback_events_for_sent_replies as sync_feedback_events_for_sent_replies_impl,
)
from app.store import (
    FAST_PATH_UNREAD_BACKOFF_TASK_ERROR,
    AutoReplyStore,
    FeedbackEvent,
    OperationLog,
    ReplyAttempt,
    ReplyError,
    ReplyTask,
    SentReply,
    UserFeedbackItem,
)
from app.setup_wizard import (
    build_wizard_status,
    check_setup_step,
    get_action_definition,
    get_step_definition,
    run_setup_action,
)
from app.setup_wizard_models import SetupStepStatus, SetupWizardEvent
from app.task_models import ProjectPriority, ProjectStatus, RiskLevel, TodoStatus
from app.user_prompt_blocks import USER_PROMPT_BLOCKS, UserPromptBlock

DISPLAY_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
from app.worker import DingTalkAutoReplyWorker


CSS = """
:root{--ink:#0a0a0a;--charcoal:#1c1c1e;--slate:#3a3a3c;--steel:#5a5a5c;--stone:#888888;--muted:#a8a8aa;--canvas:#ffffff;--surface:#f7f7f7;--surface-soft:#fafafa;--surface-code:#1c1c1e;--hairline:#e5e5e5;--hairline-soft:#ededed;--mint:#00d4a4;--mint-deep:#00b48a;--tag:#3772cf;--error:#d45656}
*{box-sizing:border-box}
body{font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;margin:0;background:var(--canvas);color:var(--ink);font-size:14px;line-height:1.5}
.sr-only{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}
.lucide-icon{display:inline-block;width:14px;height:14px;stroke:currentColor;stroke-width:2;stroke-linecap:round;stroke-linejoin:round;fill:none;flex:0 0 auto}
header{position:sticky;top:0;z-index:10;background:rgba(255,255,255,.94);border-bottom:1px solid var(--hairline);backdrop-filter:saturate(180%) blur(12px)}
.shell{width:100%;margin:0 auto;padding:0 24px}
.topbar{display:flex;align-items:center;justify-content:space-between;gap:24px;min-height:72px}
.brand{display:flex;align-items:center;gap:12px;min-width:0}
.brand-home:hover{text-decoration:none}
.brand-mark{width:28px;height:28px;border-radius:8px;background:var(--ink);box-shadow:inset 0 -8px 0 rgba(0,212,164,.26)}
h1{margin:0;color:var(--ink);font-size:18px;font-weight:600;line-height:1.35;letter-spacing:0}
.eyebrow{margin-top:2px;color:var(--steel);font-size:12px;font-weight:500;line-height:1.4}
main{width:100%;max-width:1000px;margin:0 auto;padding:20px 24px 40px}
main.main-wide{max-width:none}
a{color:var(--ink);text-decoration:none}
a:hover{text-decoration:underline;text-decoration-color:var(--mint);text-underline-offset:3px}
select{appearance:none;-webkit-appearance:none;-moz-appearance:none;background-color:var(--canvas);background-image:url("data:image/svg+xml,%3Csvg width='12' height='8' viewBox='0 0 12 8' fill='none' xmlns='http://www.w3.org/2000/svg'%3E%3Cpath d='M2 2L6 6L10 2' stroke='%235A5A5C' stroke-width='1.7' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 12px center;background-size:12px 8px;padding-right:34px!important;cursor:pointer}
select::-ms-expand{display:none}
select:hover{border-color:#cfcfcf;background-color:#fbfbfb}
select:focus{outline:0;border-color:rgba(0,180,138,.55);box-shadow:0 0 0 3px rgba(0,212,164,.14)}
select[data-custom-select-enhanced="1"]{position:absolute!important;width:1px!important;height:1px!important;margin:0!important;padding:0!important;border:0!important;opacity:0!important;pointer-events:none!important;clip:rect(0,0,0,0)!important;clip-path:inset(50%)!important}
.custom-select{position:relative;display:inline-flex;align-items:center;vertical-align:top;min-width:0}
.custom-select-trigger{display:inline-flex;align-items:center;justify-content:space-between;gap:10px;width:100%;height:32px;padding:0 10px 0 12px;border:1px solid var(--hairline);border-radius:999px;background:var(--canvas);color:var(--ink);box-shadow:0 1px 0 rgba(0,0,0,.02);font-size:12px;font-weight:750;line-height:1;white-space:nowrap;cursor:pointer}
.custom-select-trigger:hover{border-color:#cfcfcf;background:#fbfbfb}
.custom-select.open .custom-select-trigger,.custom-select-trigger:focus-visible{outline:0;border-color:rgba(0,180,138,.55);box-shadow:0 0 0 3px rgba(0,212,164,.14)}
.custom-select-label{min-width:0;overflow:hidden;text-overflow:ellipsis}
.custom-select-chevron{width:13px;height:13px;color:var(--steel);transition:transform .14s ease;flex:0 0 auto}
.custom-select.open .custom-select-chevron{transform:rotate(180deg)}
.custom-select-menu{position:fixed;z-index:80;display:grid;gap:2px;max-height:min(320px,calc(100vh - 32px));padding:5px;border:1px solid var(--hairline);border-radius:14px;background:rgba(255,255,255,.98);box-shadow:0 18px 50px rgba(0,0,0,.16);backdrop-filter:saturate(180%) blur(12px);overflow:auto}
.custom-select-option{display:flex;align-items:center;justify-content:space-between;gap:12px;width:100%;min-height:32px;padding:7px 9px;border:0;border-radius:10px;background:transparent;color:var(--charcoal);font-size:13px;font-weight:650;line-height:1.25;text-align:left;white-space:nowrap}
.custom-select-option:hover,.custom-select-option:focus-visible{outline:0;background:var(--surface-soft);color:var(--ink)}
.custom-select-option[aria-selected="true"]{background:#ddfff6;color:#005b49}
.custom-select-option[aria-disabled="true"]{color:var(--muted);cursor:default}
.custom-select-check{width:14px;height:14px;color:#005b49;opacity:0;flex:0 0 auto}
.custom-select-option[aria-selected="true"] .custom-select-check{opacity:1}
table{width:100%;border-collapse:separate;border-spacing:0;background:var(--canvas);border:1px solid var(--hairline);border-radius:8px;overflow:hidden}
th,td{border-bottom:1px solid var(--hairline-soft);padding:12px 14px;text-align:left;vertical-align:top;font-size:14px;line-height:1.45}
tr:last-child td{border-bottom:0}
th{background:var(--surface-soft);color:var(--steel);font-size:12px;font-weight:600;line-height:1.4}
.column-sized-table{table-layout:fixed}
.column-sized-table th,.column-sized-table td{overflow-wrap:anywhere;word-break:break-word}
.config-variable-table th,.config-variable-table td{padding:5px 8px}
.config-variable-table th:first-child,.config-variable-table td:first-child{width:360px}
.config-variable-table td:first-child .config-value{white-space:nowrap;word-break:normal}
.config-variable-table input[type="text"]{height:28px;padding:4px 7px;border-radius:6px;font-size:12px;line-height:1.35}
.config-key-input{font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;color:var(--steel);background:var(--surface-soft)}
.config-value-input{font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
.config-value{display:inline-flex;max-width:100%;padding:4px 8px;border-radius:7px;background:var(--surface);border:1px solid var(--hairline-soft);color:var(--charcoal);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:12px;line-height:1.45;white-space:pre-wrap;word-break:break-word}
.config-token{display:inline-flex;max-width:100%;padding:3px 7px;border-radius:6px;background:#ddfff6;border:1px solid rgba(0,180,138,.55);color:#005b49;font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:12px;font-weight:700;line-height:1.4;white-space:pre-wrap;word-break:break-word;box-shadow:0 0 0 2px rgba(0,212,164,.12)}
.system-config-table th:first-child,.system-config-table td:first-child{width:260px}
.system-config-table th:nth-child(2),.system-config-table td:nth-child(2){width:280px}
.config-collapse{border:1px solid var(--hairline);border-radius:8px;background:var(--surface-soft);margin:10px 0;overflow:hidden}
.config-collapse summary{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:10px 12px;cursor:pointer;list-style:none}
.config-collapse summary::-webkit-details-marker{display:none}
.config-collapse summary h3{margin:0;font-size:14px;line-height:1.35}
.config-collapse summary::after{content:"Show";color:var(--steel);font-size:12px;font-weight:600}
.config-collapse[open] summary{border-bottom:1px solid var(--hairline)}
.config-collapse[open] summary::after{content:"Hide"}
.config-collapse table{border:0;border-radius:0}
.config-collapse form{padding:0 0 10px}
.dynamic-preview{max-height:56px;margin:0;padding:7px 9px;font-size:12px;line-height:1.35}
.logic-list{display:grid;gap:14px}
.logic-section{border:1px solid var(--hairline);border-radius:8px;padding:16px;background:var(--surface-soft)}
.logic-section h3{margin:0 0 10px;color:var(--ink);font-size:16px;font-weight:600;line-height:1.4}
.logic-section dl{display:grid;gap:9px;margin:0}
.logic-section dt{color:var(--steel);font-size:12px;font-weight:700;line-height:1.4}
.logic-section dd{margin:2px 0 0;color:var(--charcoal);font-size:14px;line-height:1.5}
.tutorial-intro{display:grid;gap:12px;margin:0 0 14px}
.tutorial-summary{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin:0}
.tutorial-summary-item{border:1px solid var(--hairline);border-radius:8px;background:var(--surface-soft);padding:12px}
.tutorial-summary-label{display:block;margin-bottom:4px;color:var(--steel);font-size:11px;font-weight:800;line-height:1.35;text-transform:uppercase;letter-spacing:.03em}
.tutorial-summary-value{color:var(--ink);font-size:14px;font-weight:750;line-height:1.35}
.tutorial-steps{display:grid;gap:12px;margin:0;padding:0;list-style:none;counter-reset:tutorial-step}
.tutorial-step{display:grid;grid-template-columns:42px minmax(0,1fr);gap:14px;border:1px solid var(--hairline);border-radius:8px;background:var(--canvas);padding:14px;counter-increment:tutorial-step}
.tutorial-step-number{display:inline-flex;align-items:center;justify-content:center;width:32px;height:32px;border:1px solid rgba(0,180,138,.28);border-radius:8px;background:#ddfff6;color:#005b49;font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:13px;font-weight:900;line-height:1}
.tutorial-step-number::before{content:counter(tutorial-step)}
.tutorial-step-body{display:grid;gap:8px;min-width:0}
.tutorial-step-head{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;flex-wrap:wrap}
.tutorial-step h3{margin:0;color:var(--ink);font-size:16px;font-weight:750;line-height:1.35}
.tutorial-phase{display:inline-flex;align-items:center;height:24px;padding:0 8px;border:1px solid var(--hairline);border-radius:999px;background:var(--surface-soft);color:var(--steel);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:11px;font-weight:800;line-height:1;white-space:nowrap}
.tutorial-step p{margin:0;color:var(--charcoal);font-size:14px;line-height:1.5}
.tutorial-lists{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:10px}
.tutorial-list{margin:0;padding:10px 12px 10px 28px;border:1px solid var(--hairline-soft);border-radius:8px;background:var(--surface-soft);color:var(--charcoal)}
.tutorial-list li{margin:3px 0;font-size:13px;line-height:1.45}
.tutorial-command-list{display:grid;gap:6px;margin:0}
.tutorial-command-list code{display:block;padding:8px 10px;border:1px solid var(--hairline-soft);border-radius:7px;background:var(--surface-code);color:#f7f7f7;font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:12px;line-height:1.45;white-space:pre-wrap;overflow-wrap:anywhere;word-break:break-word}
.tutorial-links{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.tutorial-link{display:inline-flex;align-items:center;height:28px;padding:0 10px;border:1px solid var(--hairline);border-radius:999px;background:var(--canvas);color:var(--ink);font-size:12px;font-weight:700;line-height:1;white-space:nowrap}
.tutorial-link:hover{border-color:var(--ink);background:var(--surface-soft);text-decoration:none}
.setup-step-status{display:inline-flex;align-items:center;height:24px;padding:0 8px;border:1px solid var(--hairline);border-radius:999px;background:var(--surface-soft);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:11px;font-weight:800;line-height:1;white-space:nowrap}
.setup-status-done{background:#ddfff6;border-color:rgba(0,180,138,.46);color:#005b49}
.setup-status-running,.setup-status-checking{background:rgba(55,114,207,.10);border-color:rgba(55,114,207,.24);color:#245aa5}
.setup-status-needs_action{background:rgba(195,125,13,.12);border-color:rgba(195,125,13,.24);color:#8a5a08}
.setup-status-failed,.setup-status-blocked{background:rgba(212,86,86,.12);border-color:rgba(212,86,86,.24);color:#9a2f2f}
.setup-wizard-step form{margin:0}
@media (max-width:900px){.tutorial-summary,.tutorial-lists{grid-template-columns:1fr}.tutorial-step{grid-template-columns:1fr}.tutorial-step-number{width:30px;height:30px}}
.notification-panel{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin:12px 0}
.notification-log{max-height:260px}
.card-head{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:12px;flex-wrap:wrap}
.card-head h2{margin:0}
.tasks-page{margin:0}
.tasks-main{padding-top:12px}
.table-toolbar{display:grid;grid-template-columns:minmax(0,1fr) auto auto;align-items:center;gap:12px;margin:0 0 12px}
.table-toolbar-left,.table-toolbar-right{display:flex;align-items:center;gap:8px;min-width:0;flex-wrap:wrap}
.table-toolbar-left{flex-wrap:nowrap}
.table-toolbar-center{display:flex;align-items:center;justify-content:center;min-width:0}
.table-toolbar-right{justify-content:flex-end}
.table-toolbar-search{position:relative;display:flex;align-items:center;flex:0 1 320px;margin:0;width:320px;max-width:100%;min-width:220px}
.table-toolbar-left .custom-table-type-select{flex:0 0 138px}
.table-toolbar-search input[type="text"]{height:32px;padding:7px 32px 7px 12px;border-radius:999px;font-size:13px;line-height:1.3}
.table-search-clear{position:absolute;right:7px;display:inline-flex;align-items:center;justify-content:center;width:22px;height:22px;border-radius:999px;color:var(--steel);font-size:16px;font-weight:700;line-height:1}
.table-search-clear[hidden]{display:none}
.table-search-clear:hover{background:var(--surface-soft);color:var(--ink);text-decoration:none}
.table-type-select,.table-page-size{height:32px;border:1px solid var(--hairline);border-radius:999px;color:var(--ink);padding:0 34px 0 12px;font-size:12px;font-weight:750;line-height:32px}
.table-type-select{width:138px}
.table-page-size{min-width:102px}
.table-page-links{display:flex;align-items:center;justify-content:center;gap:3px;width:204px;min-width:0;white-space:nowrap}
.table-page-link,.table-page-arrow,.table-page-ellipsis{display:inline-flex;align-items:center;justify-content:center;height:32px;min-width:28px;padding:0 8px;border:1px solid transparent;border-radius:999px;color:var(--steel);font-size:12px;font-weight:800;line-height:1;background:transparent}
.table-page-arrow{font-size:18px}
.table-page-link:hover,.table-page-arrow:hover{border-color:var(--hairline);background:var(--surface-soft);color:var(--ink);text-decoration:none}
.table-page-link.active{border-color:rgba(0,180,138,.28);background:#ddfff6;color:#005b49}
.table-page-arrow.disabled,.table-page-ellipsis{color:var(--muted);cursor:default}
.table-toolbar-total{min-width:72px;text-align:right;color:var(--steel);font-size:12px;font-weight:700;line-height:1.35;white-space:nowrap}
.infinite-load-status{display:flex;align-items:center;justify-content:center;min-height:48px;margin:12px 0 0;color:var(--steel);font-size:12px;font-weight:700}
.infinite-load-status[hidden]{display:none}
.infinite-load-status.done{color:var(--muted)}
.tasks-count{display:none}
.todo-checklist{display:grid;gap:4px;margin:0;padding:0;list-style:none}
.todo-checklist li{display:flex;align-items:flex-start;gap:7px;min-width:0;color:var(--charcoal);font-size:13px;line-height:1.35}
.todo-check{display:inline-flex;align-items:center;justify-content:center;flex:0 0 auto;width:15px;height:15px;margin-top:1px;border:1px solid var(--hairline);border-radius:4px;color:transparent;font-size:11px;font-weight:900;line-height:1}
.todo-check.done{border-color:rgba(0,180,138,.46);background:#ddfff6;color:#005b49}
.todo-check .lucide-icon,.todo-detail-check .lucide-icon{width:11px;height:11px}
.todo-copy{display:grid;gap:2px;min-width:0}
.todo-due{color:var(--steel);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:11px;line-height:1.3}
.todo-total{color:var(--steel);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:11px;font-weight:700;line-height:1.3}
.todo-detail-list{display:grid;gap:0}
.todo-detail-item{display:grid;gap:10px;padding:12px 0;border-bottom:1px solid var(--hairline-soft)}
.todo-detail-item:first-child{padding-top:0}
.todo-detail-item:last-child{border-bottom:0;padding-bottom:0}
.todo-detail-main{display:grid;grid-template-columns:18px minmax(0,1fr);gap:10px;align-items:start}
.todo-detail-check{display:inline-flex;align-items:center;justify-content:center;width:16px;height:16px;margin-top:3px;border:1px solid var(--hairline);border-radius:4px;background:var(--canvas);color:transparent;font-size:11px;font-weight:900;line-height:1}
.todo-detail-check.done{border-color:rgba(0,180,138,.46);background:#ddfff6;color:#005b49}
.todo-detail-body{display:grid;gap:7px;min-width:0}
.todo-detail-title-row{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;min-width:0}
.todo-detail-title{min-width:0;margin:0;color:var(--ink);font-size:15px;font-weight:760;line-height:1.4;overflow-wrap:anywhere;word-break:break-word}
.todo-detail-meta{display:flex;flex-wrap:wrap;gap:5px 10px;color:var(--steel);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:11px;font-weight:700;line-height:1.35}
.todo-detail-fields{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:2px}
.todo-detail-field{min-width:0}
.todo-detail-label{margin-bottom:2px;color:var(--steel);font-size:11px;font-weight:800;line-height:1.25;text-transform:uppercase}
.todo-detail-value{color:var(--charcoal);font-size:13px;line-height:1.45;overflow-wrap:anywhere;word-break:break-word}
.detail-pill-list{display:flex;align-items:flex-start;gap:6px;flex-wrap:wrap;min-width:0}
.detail-pill{display:inline-flex;align-items:center;min-height:24px;padding:3px 9px;border:1px solid var(--hairline);border-radius:999px;background:var(--surface-soft);color:var(--charcoal);font-size:12px;font-weight:800;line-height:1.25;overflow-wrap:anywhere;word-break:break-word}
.todo-detail-followups{display:grid;gap:8px;margin-left:28px;padding:10px 0 0 12px;border-left:2px solid rgba(55,114,207,.24);color:var(--charcoal)}
.todo-followup-heading{color:var(--steel);font-size:12px;font-weight:800;line-height:1.25}
.todo-followup-list{display:grid;gap:8px;margin:0;padding:0;list-style:none}
.todo-followup-item{display:flex;min-width:0}
.todo-followup-bubble{display:grid;gap:7px;width:min(760px,100%);padding:10px 12px;border:1px solid rgba(55,114,207,.16);border-radius:12px 12px 12px 4px;background:#f5faff;color:var(--charcoal);box-shadow:0 1px 0 rgba(17,24,39,.03)}
.todo-followup-head{display:flex;align-items:center;gap:7px;min-width:0;flex-wrap:wrap}
.todo-followup-recipient{min-width:0;color:var(--ink);font-size:12px;font-weight:800;line-height:1.25;overflow-wrap:anywhere;word-break:break-word}
.todo-followup-status{display:inline-flex;align-items:center;height:20px;padding:0 7px;border:1px solid rgba(55,114,207,.18);border-radius:999px;background:var(--canvas);color:#245aa5;font-size:11px;font-weight:800;line-height:1;white-space:nowrap}
.todo-followup-time{margin-left:auto;color:var(--steel);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:11px;font-weight:700;line-height:1.3;white-space:nowrap}
.todo-followup-message{color:var(--ink);font-size:13px;line-height:1.5;overflow-wrap:anywhere;word-break:break-word}
.todo-followup-target{color:var(--steel);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:11px;font-weight:700;line-height:1.35;overflow-wrap:anywhere;word-break:break-word}
.progress-cell{display:grid;gap:5px;min-width:0}
.progress-meter{height:6px;border-radius:999px;background:var(--surface-soft);overflow:hidden}
.progress-bar{height:100%;border-radius:999px;background:#3772cf}
.progress-label{color:var(--steel);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:11px;font-weight:800;line-height:1.25;white-space:nowrap}
.task-state{display:inline-flex;align-items:center;height:24px;padding:0 8px;border:1px solid var(--hairline);border-radius:999px;background:var(--surface-soft);font-size:12px;font-weight:800;line-height:1;white-space:nowrap}
.task-state.completed{background:#ddfff6;border-color:rgba(0,180,138,.46);color:#005b49}
.task-state.over-due{background:rgba(212,86,86,.12);border-color:rgba(212,86,86,.24);color:#9a2f2f}
.task-state.in-progress{background:rgba(55,114,207,.10);border-color:rgba(55,114,207,.24);color:#245aa5}
.task-state.not-started{background:var(--surface-soft);color:var(--steel)}
.tasks-table-component{width:100%;overflow:hidden;border:1px solid var(--hairline);border-radius:16px;background:var(--canvas);box-shadow:0 12px 34px rgba(0,0,0,.045)}
.tasks-table-scroll{width:100%;overflow-x:auto}
.tasks-table{width:100%;min-width:1540px;border:0;border-collapse:separate;border-spacing:0;background:var(--canvas);table-layout:fixed}
.tasks-table th,.tasks-table td{border-right:0;border-bottom:1px solid var(--hairline-soft);padding:13px 14px;vertical-align:top}
.tasks-table th{position:sticky;top:0;z-index:1;background:rgba(250,250,250,.96);backdrop-filter:saturate(180%) blur(10px);color:var(--steel);font-size:12px;font-weight:800;line-height:1.25;white-space:nowrap}
.tasks-table-sort{display:inline-flex;align-items:center;gap:5px;margin:0;padding:0;border:0;background:transparent;color:inherit;font:inherit;font-weight:800;line-height:1;cursor:pointer}
.tasks-table-sort:hover{color:var(--ink)}
.tasks-table-sort-icon{display:inline-flex;align-items:center;justify-content:center;width:12px;height:12px;color:var(--muted)}
.tasks-table-sort.active{color:var(--ink)}
.tasks-table-sort.active .tasks-table-sort-icon{color:var(--mint-deep)}
.tasks-table tbody tr{transition:background-color .14s ease}
.tasks-table tbody tr[data-task-href]{cursor:pointer}
.tasks-table tbody tr:hover{background:#fafafa}
.tasks-table tbody tr[data-task-href]:focus-visible{outline:2px solid rgba(0,180,138,.42);outline-offset:-2px;background:#fafafa}
.tasks-table tbody tr:last-child td{border-bottom:0}
.tasks-table a.task-table-link{color:var(--ink);font-weight:760;line-height:1.35;text-decoration:none;overflow-wrap:anywhere;word-break:break-word}
.tasks-table a.task-table-link:hover{text-decoration:underline;text-decoration-color:var(--mint);text-underline-offset:3px}
.task-table-meta{display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-top:7px;color:var(--steel);font-size:12px;font-weight:650}
.task-table-meta .pill{min-height:22px;padding:2px 8px;font-size:11px}
.task-table-text{display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:3;overflow:hidden;color:var(--charcoal);font-size:13px;line-height:1.45;overflow-wrap:anywhere;word-break:break-word}
.task-table-progress{min-width:120px}
.task-table-empty{padding:26px;border-top:1px solid var(--hairline-soft);background:var(--surface-soft);color:var(--steel);text-align:center;font-size:13px;font-weight:650}
.task-project-title{font-weight:700;overflow-wrap:anywhere;word-break:break-word}
.task-cell-text{display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:4;overflow:hidden;white-space:normal;overflow-wrap:anywhere;word-break:break-word}
.codex-session-table tbody tr[data-session-href]{cursor:pointer;transition:background-color .14s ease}
.codex-session-table tbody tr[data-session-href]:hover{background:#fafafa}
.codex-session-table tbody tr[data-session-href]:focus-visible{outline:2px solid rgba(0,180,138,.42);outline-offset:-2px;background:#fafafa}
.compact-button{display:inline-flex;align-items:center;height:30px;padding:0 12px;border:1px solid var(--hairline);border-radius:999px;background:var(--canvas);color:var(--ink);font-size:13px;font-weight:500;line-height:1;white-space:nowrap}
.compact-button:hover{border-color:var(--ink);background:var(--surface-soft)}
.agent-log-button{display:inline-flex;align-items:center;height:34px;padding:0 14px;border:1px solid rgba(55,114,207,.38);border-radius:999px;background:#3772cf;color:#fff;font-size:13px;font-weight:700;line-height:1;white-space:nowrap;box-shadow:0 6px 18px rgba(55,114,207,.18)}
.agent-log-button:hover{background:#245aa5;color:#fff;text-decoration:none}
.pagination{display:flex;align-items:center;justify-content:space-between;gap:12px;margin:0 0 12px;padding:8px 10px;border:1px solid var(--hairline);border-radius:8px;background:var(--surface-soft);flex-wrap:wrap}
.pagination.bottom{margin:12px 0 0}
.pagination-status{display:flex;align-items:center;gap:8px;min-width:0;flex-wrap:wrap}
.pagination-range{color:var(--ink);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:13px;font-weight:800;line-height:1.3}
.pagination-page{display:inline-flex;align-items:center;height:24px;padding:0 8px;border:1px solid rgba(0,180,138,.28);border-radius:999px;background:#ddfff6;color:#005b49;font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:12px;font-weight:800;line-height:1}
.pagination-total{color:var(--steel);font-size:12px;font-weight:600;line-height:1.35}
.pagination-actions{display:flex;align-items:center;gap:4px;padding:3px;border:1px solid var(--hairline);border-radius:999px;background:var(--canvas);flex-wrap:nowrap}
.pagination-button{display:inline-flex;align-items:center;justify-content:center;height:28px;min-width:34px;padding:0 10px;border:1px solid transparent;border-radius:999px;background:transparent;color:var(--steel);font-size:12px;font-weight:700;line-height:1;white-space:nowrap}
.pagination-button:hover{border-color:var(--hairline);background:var(--surface-soft);color:var(--ink);text-decoration:none}
.pagination-arrow{min-width:28px;padding:0 8px;font-size:16px}
.pagination-button.is-disabled{color:var(--muted);background:var(--surface-soft);cursor:default}
.pagination-button.is-disabled:hover{border-color:transparent;color:var(--muted);background:var(--surface-soft)}
.history-chart-card{padding:16px 18px;margin:0 0 12px}
.history-chart-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:10px;flex-wrap:wrap}
.history-chart-title{margin:0;color:var(--ink);font-size:16px;font-weight:700;line-height:1.35}
.history-chart-subtitle{color:var(--steel);font-size:12px;font-weight:600;line-height:1.4}
.history-chart{width:100%;height:260px}
.history-chart-empty{display:flex;align-items:center;justify-content:center;height:180px;border:1px dashed var(--hairline);border-radius:8px;color:var(--steel);background:var(--surface-soft);font-size:13px}
.attempt-feed{display:grid;gap:8px}
.attempt-item{background:var(--canvas);border:1px solid var(--hairline);border-radius:8px;padding:10px 12px;transition:background-color .14s ease,border-color .14s ease,box-shadow .14s ease}
.attempt-item[data-href]{cursor:pointer}
.attempt-item[data-href]:hover{background:var(--surface-soft);border-color:#d8d8d8}
.attempt-item[data-href]:focus{outline:0}
.attempt-item[data-href]:focus-visible{border-color:rgba(0,180,138,.55);box-shadow:0 0 0 3px rgba(0,212,164,.14)}
.attempt-head{display:flex;align-items:center;justify-content:space-between;gap:12px;min-width:0}
.attempt-title{display:flex;align-items:center;gap:7px;min-width:0;flex-wrap:nowrap}
.attempt-side{display:flex;align-items:center;gap:10px;flex:0 0 auto}
.attempt-id{font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:13px;font-weight:700;color:var(--ink)}
.attempt-main{font-size:14px;font-weight:600;color:var(--ink);min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.attempt-meta{color:var(--steel);font-size:13px;line-height:1.4;white-space:nowrap}
.attempt-time{color:var(--stone);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:12px;line-height:1.4;text-align:right;white-space:nowrap}
.attempt-lines{display:grid;gap:4px;margin-top:8px}
.attempt-line{display:grid;grid-template-columns:24px minmax(0,1fr);gap:8px;align-items:start;min-width:0}
.attempt-label{color:var(--steel);font-size:12px;font-weight:700;line-height:1.45}
.attempt-copy{color:var(--charcoal);font-size:13px;line-height:1.45;display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:2;overflow:hidden}
.attempt-reaction-copy{display:inline-flex;align-items:center;width:max-content;max-width:100%;padding:4px 9px;border-radius:999px;background:#fff4d6;border:1px solid #f4d06f;color:#5f4200;font-size:13px;line-height:1.2;-webkit-line-clamp:1;box-shadow:inset 0 -1px 0 rgba(95,66,0,.08)}
.attempt-foot{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-top:6px;flex-wrap:wrap}
.attempt-actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.attempt-row-actions{display:flex;align-items:center;gap:6px;flex-wrap:wrap}
.attempt-row-actions form{display:inline-flex;margin:0}
.attempt-row-actions .compact-button,.attempt-row-actions button{display:inline-flex;align-items:center;justify-content:center;width:96px;height:30px;padding:0 10px;border:1px solid var(--hairline);border-radius:999px;background:var(--canvas);color:var(--ink);font-size:12px;font-weight:700;line-height:1;white-space:nowrap}
.attempt-row-actions .compact-button:hover,.attempt-row-actions button:hover{border-color:var(--ink);text-decoration:none}
.attempt-row-actions button.rerun{border-color:rgba(55,114,207,.34);color:#245aa5;background:rgba(55,114,207,.10)}
.attempt-row-actions button.rerun:hover{background:rgba(55,114,207,.16)}
.attempt-row-actions button.danger{border-color:rgba(212,86,86,.32);color:#9a2f2f;background:rgba(212,86,86,.08)}
.attempt-row-actions button.danger:hover{background:rgba(212,86,86,.14)}
.attempt-row-actions .open-dingtalk-action{border-color:rgba(0,180,138,.38);color:#005b49;background:#ddfff6}
.attempt-row-actions .open-dingtalk-action:hover{background:#cafff1}
.attempt-row-actions button.feedback-modal-action{border-color:rgba(55,114,207,.26);color:#245aa5;background:#f5faff}
.attempt-row-actions button.feedback-modal-action:hover{background:#edf5ff}
.attempt-row-actions .disabled-action{display:inline-flex;align-items:center;justify-content:center;width:96px;height:30px;padding:0 10px;border:1px solid var(--hairline);border-radius:999px;background:var(--surface-soft);color:var(--muted);font-size:12px;font-weight:700;line-height:1;white-space:nowrap}
.attempt-warning{color:#8a2626;font-size:12px;line-height:1.4}
.attempt-conversation-banner{display:flex;align-items:center;justify-content:space-between;gap:14px;border:1px solid rgba(0,180,138,.34);background:#f3fffb}
.attempt-conversation-left{display:flex;align-items:center;gap:14px;min-width:0}
.attempt-banner-actions{display:flex;align-items:center;justify-content:flex-end;gap:8px;flex:0 0 auto;flex-wrap:wrap}
.attempt-conversation-label{display:inline-flex;align-items:center;height:28px;padding:0 10px;border-radius:999px;background:#ddfff6;border:1px solid rgba(0,180,138,.42);color:#005b49;font-size:12px;font-weight:800;white-space:nowrap}
.attempt-conversation-main{min-width:0}
.attempt-conversation-title{color:var(--ink);font-size:20px;font-weight:750;line-height:1.3;word-break:break-word}
.attempt-conversation-sub{margin-top:2px;color:var(--steel);font-size:12px;font-weight:600;line-height:1.4}
.attempt-detail-page-head{display:grid;justify-items:start;gap:8px;margin:0 0 14px}
.attempt-detail-back{display:inline-flex;align-items:center;gap:7px;width:max-content;height:34px;padding:0 12px;border:1px solid var(--hairline);border-radius:999px;background:var(--canvas);color:var(--ink);font-size:13px;font-weight:700;line-height:1;white-space:nowrap}
.attempt-detail-back:hover{border-color:var(--ink);background:var(--surface-soft);text-decoration:none}
.attempt-detail-heading{display:grid;gap:4px;min-width:0}
.attempt-detail-heading h2{margin:0;color:var(--ink);font-size:24px;font-weight:760;line-height:1.25}
.attempt-detail-heading p{margin:0;color:var(--steel);font-size:13px;font-weight:600;line-height:1.4}
.attempt-detail-grid{display:flex;align-items:stretch;gap:8px;overflow-x:auto;padding-bottom:2px}
.attempt-detail-cell{flex:0 0 auto;min-width:118px;max-width:260px;padding:8px 10px;border:1px solid var(--hairline);border-radius:8px;background:var(--surface-soft)}
.attempt-detail-cell:first-child{min-width:220px}
.attempt-detail-label{margin-bottom:3px;color:var(--steel);font-size:11px;font-weight:800;line-height:1.3;text-transform:uppercase}
.attempt-detail-value{color:var(--ink);font-size:12px;font-weight:650;line-height:1.35;word-break:break-word}
.feedback-chip{display:inline-flex;align-items:center;max-width:100%;min-height:24px;padding:3px 9px;border-radius:999px;background:#ddfff6;border:1px solid rgba(0,180,138,.42);color:#005b49;font-size:12px;font-weight:700;line-height:1.35;white-space:nowrap}
.feedback-card{border-color:rgba(0,180,138,.28);background:linear-gradient(180deg,#ffffff 0%,#f6fffc 100%)}
.feedback-event{border:1px solid var(--hairline);border-radius:8px;background:var(--canvas);padding:12px;margin-top:10px}
.feedback-event-head{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-bottom:8px}
.feedback-rating{display:inline-flex;align-items:center;min-height:26px;padding:4px 10px;border-radius:999px;background:rgba(0,212,164,.12);border:1px solid rgba(0,180,138,.28);color:#005b49;font-size:13px;font-weight:700}
.feedback-comment{font-size:14px;color:var(--charcoal);white-space:pre-wrap}
.feedback-token{font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;color:var(--steel);font-size:12px;word-break:break-all}
.user-feedback-table th:nth-child(1),.user-feedback-table td:nth-child(1){width:112px}
.user-feedback-table th:nth-child(2),.user-feedback-table td:nth-child(2){width:100px}
.user-feedback-table th:nth-child(4),.user-feedback-table td:nth-child(4){width:150px}
.user-feedback-table th:nth-child(5),.user-feedback-table td:nth-child(5){width:190px}
.user-feedback-comment{font-weight:600;color:var(--ink)}
.user-feedback-context{margin-top:4px;color:var(--steel);font-size:12px;line-height:1.4}
.user-feedback-actions{display:flex;align-items:center;gap:8px;flex-wrap:nowrap;white-space:nowrap}
.user-feedback-actions form{display:inline-flex;margin:0}
.user-feedback-actions button{display:inline-flex;align-items:center;height:30px;padding:0 12px;border:1px solid var(--hairline);border-radius:999px;background:var(--canvas);color:var(--ink);font-size:13px;font-weight:500;line-height:1;white-space:nowrap}
.user-feedback-actions button:hover{border-color:var(--ink);background:var(--surface-soft)}
.audit-tool-list{display:grid;gap:12px;margin:8px 24px 24px}
.audit-tool-event{border:1px solid var(--hairline);border-radius:8px;background:var(--canvas);padding:12px}
.audit-tool-head{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-bottom:10px}
.audit-tool-title{display:flex;align-items:center;gap:8px;min-width:0;color:var(--ink);font-size:14px;font-weight:750}
.audit-tool-index{font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;color:var(--steel);font-size:12px;font-weight:700}
.audit-tool-command{max-width:100%;font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;color:var(--steel);font-size:12px;line-height:1.4;word-break:break-word}
.audit-tool-io{display:grid;gap:8px}
.audit-tool-section{display:grid;gap:4px}
.audit-tool-label{color:var(--steel);font-size:12px;font-weight:800;text-transform:uppercase;letter-spacing:.03em}
.audit-tool-pre{margin:0;max-height:420px;overflow:auto;border:1px solid var(--hairline);border-radius:7px;background:var(--surface-soft);padding:9px 10px;color:var(--charcoal);font-size:12px;line-height:1.45;white-space:pre-wrap;word-break:break-word}
.audit-tool-meta{display:grid;grid-template-columns:120px minmax(0,1fr);gap:6px 12px;margin:8px 0 10px;padding:10px;border:1px solid var(--hairline-soft);border-radius:7px;background:var(--surface-soft)}
.audit-tool-meta-label{color:var(--steel);font-size:12px;font-weight:800;line-height:1.35;text-transform:uppercase}
.audit-tool-meta-value{min-width:0;color:var(--charcoal);font-size:13px;line-height:1.4;overflow-wrap:anywhere;word-break:break-word}
.audit-tool-output{border:1px solid var(--hairline);border-radius:7px;background:var(--surface-soft);overflow:hidden}
.audit-tool-output summary{display:grid;grid-template-columns:auto minmax(0,1fr);gap:10px;align-items:center;padding:8px 10px;cursor:pointer}
.audit-tool-output summary::after{display:none}
.audit-tool-output-preview{min-width:0;color:var(--steel);font-size:12px;line-height:1.35;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.audit-tool-output-body{max-height:420px;overflow:auto;border-top:1px solid var(--hairline);padding:10px}
.audit-tool-output-body pre{margin:0;border:0;border-radius:0;background:transparent;padding:0}
.audit-tool-rendered-text{color:var(--charcoal);font-size:13px;line-height:1.5}
.audit-tool-rendered-text p{margin:0 0 8px}
.audit-tool-rendered-text ul{margin:0 0 8px 18px;padding:0}
.audit-tool-rendered-text li{margin:2px 0}
.audit-tool-rendered-text h1,.audit-tool-rendered-text h2,.audit-tool-rendered-text h3{margin:8px 0 6px;color:var(--ink);font-weight:700;line-height:1.3}
.audit-tool-rendered-text h1{font-size:17px}
.audit-tool-rendered-text h2{font-size:16px}
.audit-tool-rendered-text h3{font-size:15px}
.audit-tool-count{color:var(--steel);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:12px;font-weight:700;line-height:1.35}
.attempt-info{position:relative;display:inline-flex;align-items:center;justify-content:center;width:16px;height:16px;border:1px solid #d29a12;border-radius:50%;color:#8a5a08;background:#fff3c4;font-size:11px;font-weight:700;line-height:1;cursor:help;flex:0 0 auto}
.attempt-info:hover,.attempt-info:focus{background:#ffe7a3;border-color:#b77908;outline:0}
.attempt-info::after{content:attr(data-tooltip);display:none;position:absolute;left:0;bottom:calc(100% + 8px);z-index:30;width:max-content;max-width:min(320px,calc(100vw - 48px));padding:7px 9px;border-radius:6px;background:#1f2937;color:#fff;box-shadow:0 8px 24px rgba(15,23,42,.18);font-size:12px;font-weight:500;line-height:1.4;text-align:left;white-space:normal}
.attempt-info::before{content:"";display:none;position:absolute;left:4px;bottom:calc(100% + 3px);z-index:31;border:5px solid transparent;border-top-color:#1f2937}
.attempt-info:hover::after,.attempt-info:focus::after,.attempt-info:hover::before,.attempt-info:focus::before{display:block}
.nav{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.nav-item{display:inline-flex;align-items:center;height:36px;padding:0 14px;border:1px solid var(--hairline);border-radius:999px;background:var(--canvas);color:var(--steel);font-size:14px;font-weight:500}
a.nav-item:hover{color:var(--ink);text-decoration:none;border-color:var(--ink)}
.nav-item.active{background:var(--ink);border-color:var(--ink);color:#fff;cursor:default}
.nav-badge{display:inline-flex;align-items:center;justify-content:center;min-width:18px;height:18px;margin-left:7px;padding:0 5px;border-radius:999px;background:#d45656;color:#fff;font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:11px;font-weight:800;line-height:1}
.prompt-tabs{display:inline-flex;align-items:center;gap:6px;padding:4px;border:1px solid var(--hairline);border-radius:999px;background:var(--surface-soft);margin:0 0 12px}
.prompt-tab{display:inline-flex;align-items:center;height:32px;padding:0 13px;border-radius:999px;color:var(--steel);font-size:13px;font-weight:600}
.prompt-tab:hover{text-decoration:none;color:var(--ink)}
.prompt-tab.active{background:var(--ink);color:#fff}
.config-hero{display:grid;grid-template-columns:minmax(0,1.2fr) minmax(280px,.8fr);gap:16px;align-items:stretch}
.config-hero-copy{display:grid;gap:10px;align-content:start}
.config-hero-copy h2{font-size:24px;margin:0}
.config-summary-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.config-stat{border:1px solid var(--hairline-soft);border-radius:10px;background:var(--surface-soft);padding:12px}
.config-stat-label{color:var(--steel);font-size:12px;font-weight:700;line-height:1.35}
.config-stat-value{margin-top:4px;color:var(--ink);font-size:20px;font-weight:760;line-height:1.2}
.domain-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}
.domain-card{border:1px solid var(--hairline);border-radius:10px;background:var(--canvas);padding:16px}
.domain-card h3{margin:0 0 8px;color:var(--ink);font-size:16px;font-weight:720;line-height:1.35}
.domain-card p{margin:6px 0;color:var(--steel)}
.domain-card ul{margin:10px 0 0;padding-left:18px;color:var(--charcoal)}
.domain-card li{margin:4px 0}
.domain-card-wide{grid-column:1/-1}
.config-kv{display:grid;gap:8px}
.config-kv-row{display:grid;grid-template-columns:170px minmax(0,1fr);gap:12px;align-items:start;padding:9px 0;border-bottom:1px solid var(--hairline-soft)}
.config-kv-row:last-child{border-bottom:0}
.config-kv-label{color:var(--steel);font-size:12px;font-weight:760;line-height:1.35}
.config-kv-value{min-width:0;color:var(--charcoal);font-size:13px;line-height:1.45;overflow-wrap:anywhere;word-break:break-word}
.config-stage-list{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-top:12px}
.config-stage{border:1px solid var(--hairline-soft);border-radius:10px;background:var(--surface-soft);padding:12px}
.config-stage-num{color:var(--stone);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:12px;font-weight:800}
.config-stage-title{margin-top:5px;color:var(--ink);font-size:14px;font-weight:760}
.config-stage-desc{margin-top:3px;color:var(--steel);font-size:12px;line-height:1.4}
.config-note{border:1px solid rgba(0,180,138,.24);border-radius:10px;background:rgba(0,212,164,.08);padding:12px;color:#006b55}
.config-warning-note{border-color:rgba(195,125,13,.26);background:rgba(195,125,13,.08);color:#7a4d05}
.config-collapse-spaced{margin-top:14px}
.dingtalk-connection-card{border-color:rgba(0,180,138,.28);background:linear-gradient(135deg,#f5fffc 0%,#fff 54%)}
.dingtalk-connection-head{display:flex;align-items:flex-start;justify-content:space-between;gap:18px;flex-wrap:wrap}
.dingtalk-account{display:grid;grid-template-columns:auto minmax(0,1fr);gap:14px;align-items:start;min-width:280px}
.dingtalk-status-dot{width:16px;height:16px;border-radius:50%;margin-top:6px;background:#00a884;box-shadow:0 0 0 7px rgba(0,212,164,.13)}
.dingtalk-status-dot.needs-action{background:#c37d0d;box-shadow:0 0 0 7px rgba(195,125,13,.13)}
.dingtalk-eyebrow{color:var(--steel);font-size:12px;font-weight:800;line-height:1.35}
.dingtalk-account h2{margin:2px 0 4px;font-size:26px;line-height:1.15}
.dingtalk-account-id{color:var(--charcoal);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:13px;overflow-wrap:anywhere}
.dingtalk-meta{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:10px}
.dingtalk-action-row{display:flex;align-items:center;gap:8px;flex-wrap:wrap;justify-content:flex-end}
.secondary-button{display:inline-flex;align-items:center;justify-content:center;min-height:36px;padding:0 14px;border:1px solid var(--hairline);border-radius:999px;background:#fff;color:var(--ink);font-size:14px;font-weight:650}
.secondary-button:hover{text-decoration:none;border-color:var(--ink)}
.dingtalk-checks{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin-top:18px}
.dingtalk-check{border:1px solid var(--hairline-soft);border-radius:10px;background:rgba(255,255,255,.78);padding:12px}
.dingtalk-check-label{color:var(--steel);font-size:12px;font-weight:800;line-height:1.35}
.dingtalk-check-value{margin-top:4px;color:var(--ink);font-size:15px;font-weight:740;line-height:1.35}
.dingtalk-check-desc{margin-top:4px;color:var(--steel);font-size:12px;line-height:1.45}
.memory-connection-card{border-color:rgba(55,114,207,.24);background:linear-gradient(135deg,#f6f9ff 0%,#fff 56%)}
.memory-status-dot{width:16px;height:16px;border-radius:50%;margin-top:6px;background:#3772cf;box-shadow:0 0 0 7px rgba(55,114,207,.12)}
.memory-status-dot.needs-action{background:#c37d0d;box-shadow:0 0 0 7px rgba(195,125,13,.13)}
.memory-source-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin-top:18px}
.memory-source-card{border:1px solid var(--hairline-soft);border-radius:10px;background:rgba(255,255,255,.82);padding:12px}
.memory-source-label{color:var(--steel);font-size:12px;font-weight:800;line-height:1.35}
.memory-source-value{margin-top:4px;color:var(--ink);font-size:15px;font-weight:740;line-height:1.35;overflow-wrap:anywhere}
.memory-source-desc{margin-top:4px;color:var(--steel);font-size:12px;line-height:1.45}
@media (max-width: 920px){.config-hero,.domain-grid{grid-template-columns:1fr}.config-stage-list{grid-template-columns:repeat(2,minmax(0,1fr))}.config-summary-grid{grid-template-columns:1fr}}
@media (max-width: 920px){.dingtalk-checks,.memory-source-grid{grid-template-columns:1fr}.dingtalk-action-row{justify-content:flex-start}}
@media (max-width: 560px){.config-stage-list{grid-template-columns:1fr}.config-kv-row{grid-template-columns:1fr;gap:4px}.dingtalk-account{grid-template-columns:1fr}.dingtalk-status-dot{margin-top:0}}
.pill{display:inline-flex;align-items:center;min-height:24px;padding:3px 9px;border-radius:999px;background:var(--surface);color:var(--steel);border:1px solid var(--hairline);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:12px;line-height:1.3;white-space:nowrap}
.status-sent{background:rgba(0,212,164,.12);color:#006b55;border-color:rgba(0,180,138,.28)}
.status-resolved{background:rgba(0,212,164,.12);color:#006b55;border-color:rgba(0,180,138,.28)}
.status-pending,.status-processing,.status-commented{background:rgba(55,114,207,.10);color:#245aa5;border-color:rgba(55,114,207,.24)}
.status-skipped{background:var(--surface);color:var(--stone)}
.status-failed,.status-blocked,.status-active{background:rgba(212,86,86,.12);color:#9a2f2f;border-color:rgba(212,86,86,.24)}
.status-action{background:var(--surface);color:var(--steel);border-color:var(--hairline)}
.status-action .lucide-icon{width:13px;height:13px;margin-right:4px}
.action-state-sent,.action-state-accepted,.action-state-approved,.action-state-resolved{background:rgba(0,212,164,.12);color:#006b55;border-color:rgba(0,180,138,.28)}
.action-state-skipped{background:var(--surface);color:var(--stone);border-color:var(--hairline)}
.action-state-pending,.action-state-processing,.action-state-dry-run,.action-state-commented{background:rgba(55,114,207,.10);color:#245aa5;border-color:rgba(55,114,207,.24)}
.action-state-tentative,.action-state-returned{background:rgba(195,125,13,.12);color:#8a5a08;border-color:rgba(195,125,13,.24)}
.action-state-failed,.action-state-blocked,.action-state-declined,.action-state-rejected{background:rgba(212,86,86,.12);color:#9a2f2f;border-color:rgba(212,86,86,.24)}
.log-feed{display:grid;gap:8px}
.log-item{display:grid;gap:8px;padding:11px 12px;border:1px solid var(--hairline);border-radius:8px;background:var(--canvas)}
.log-main{display:grid;gap:8px;min-width:0}
.log-head{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:10px;align-items:start}
.log-title{display:flex;align-items:center;gap:8px;min-width:0;flex-wrap:wrap}
.log-action{min-width:0;color:var(--ink);font-size:14px;font-weight:760;line-height:1.35;overflow-wrap:anywhere;word-break:break-word}
.log-time{color:var(--steel);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:11px;font-weight:700;line-height:1.35;text-align:right;white-space:nowrap}
.log-meta{display:flex;gap:6px 10px;flex-wrap:wrap;min-width:0;color:var(--steel);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:11px;font-weight:700;line-height:1.35}
.log-context{min-width:0;overflow-wrap:anywhere;word-break:break-word}
.log-body{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:8px}
.log-body.single{grid-template-columns:1fr}
.log-field{min-width:0;padding:8px 9px;border:1px solid var(--hairline-soft);border-radius:7px;background:var(--surface-soft)}
.log-label{margin-bottom:3px;color:var(--steel);font-size:11px;font-weight:800;line-height:1.25;text-transform:uppercase}
.log-value{display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:3;overflow:hidden;color:var(--charcoal);font-size:12px;line-height:1.45;overflow-wrap:anywhere;word-break:break-word}
.quality-warning{border-color:rgba(212,86,86,.28);background:rgba(212,86,86,.08)}
.quality-warning ul{margin:8px 0 0;padding-left:20px;color:#8a2626}
.context-only-info{display:inline-flex;align-items:center;gap:8px}
.card{min-width:0;background:var(--canvas);border:1px solid var(--hairline);border-radius:8px;padding:24px;margin:16px 0}
.card h2{margin:0 0 14px;color:var(--ink);font-size:18px;font-weight:600;line-height:1.4;letter-spacing:0}
.card p{margin:8px 0}
.review-grid{display:grid;grid-template-columns:minmax(0,1.25fr) minmax(340px,.75fr);gap:16px;align-items:start;margin:16px 0}
.review-grid.single{grid-template-columns:1fr}
.review-grid .card{margin:0}
.review-side{display:grid;gap:16px}
.reply-pre{min-height:188px;background:var(--surface-soft);border-color:var(--hairline);font-size:14px;line-height:1.55;overflow-wrap:anywhere;word-break:break-word}
.reply-meta{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:12px}
.trigger-pre{min-height:0;margin:0 0 14px;background:var(--surface-soft);border-color:var(--hairline);font-size:14px;line-height:1.55;overflow-wrap:anywhere;word-break:break-word}
.codex-reason{margin:0 0 14px;padding:12px 14px;border:1px solid rgba(55,114,207,.22);border-radius:8px;background:rgba(55,114,207,.08);color:var(--charcoal);font-size:14px;line-height:1.5;white-space:pre-wrap;overflow-wrap:anywhere;word-break:break-word}
.compact-card{padding:16px}
.compact-card h2{font-size:16px;margin-bottom:10px}
.collapsible-card{padding:0;overflow:hidden}
.collapsible-card summary{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:16px 24px;cursor:pointer}
.collapsible-card summary h2{margin:0;font-size:18px}
.collapsible-card summary::after{content:"Show";color:var(--steel);font-size:12px;font-weight:600}
.collapsible-card[open] summary{border-bottom:1px solid var(--hairline)}
.collapsible-card[open] summary::after{content:"Hide"}
.collapsible-card pre{border:0;border-radius:0;margin:0}
.event{background:var(--canvas);border:1px solid var(--hairline);border-radius:8px;margin:16px 0;overflow:hidden}
.event summary{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:14px 16px;cursor:pointer;list-style:none}
.event summary::-webkit-details-marker{display:none}
.event-title{min-width:0;font-size:15px;font-weight:600;color:var(--ink);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.event-preview{margin-top:3px;color:var(--steel);font-size:12px;font-weight:400;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.event time{flex:0 0 auto;color:var(--stone);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:12px}
.event pre{border:0;border-top:1px solid var(--hairline);border-radius:0;margin:0}
.grid{display:grid;grid-template-columns:180px 1fr;gap:10px 18px}
.grid .muted{font-size:12px;font-weight:600}
pre{max-width:100%;white-space:pre-wrap;overflow-wrap:anywhere;word-break:break-word;background:var(--surface);border:1px solid var(--hairline);border-radius:8px;padding:16px;overflow:auto;color:var(--charcoal);font-family:"Geist Mono","SF Mono",Menlo,Consolas,monospace;font-size:13px;line-height:1.55}
.json-pre{background:#fbfbfb;color:var(--charcoal)}
.json-key{color:#7b3fb2}
.json-string{color:#0b6b50}
.json-number{color:#9a5b00}
.json-bool{color:#1f5fbf}
.json-null{color:#8a2626}
textarea,input[type="text"]{width:100%;box-sizing:border-box;background:var(--canvas);color:var(--ink);border:1px solid var(--hairline);border-radius:8px;padding:12px 14px;font:inherit}
textarea{min-height:104px;resize:vertical}
textarea:focus,input[type="text"]:focus{outline:0;border-color:var(--mint);box-shadow:0 0 0 3px rgba(0,212,164,.16)}
button{background:var(--ink);color:#fff;border:0;border-radius:999px;padding:10px 18px;font-size:14px;font-weight:500;line-height:1.3}
label{display:block;margin:14px 0 7px;color:var(--slate);font-size:13px;font-weight:600}
.review-link{display:inline-flex;align-items:center;height:30px;padding:0 12px;border:1px solid var(--hairline);border-radius:999px;background:var(--canvas);color:var(--ink);font-size:13px;font-weight:500;white-space:nowrap}
.review-link:hover{text-decoration:none;border-color:var(--ink);background:var(--surface-soft)}
.danger{background:#9f1d1d}
.feedback-dialog{width:min(640px,calc(100vw - 32px));padding:0;border:1px solid var(--hairline);border-radius:16px;background:var(--canvas);box-shadow:0 28px 90px rgba(0,0,0,.28)}
.feedback-dialog::backdrop{background:rgba(0,0,0,.34);backdrop-filter:blur(2px)}
.feedback-dialog-card{margin:0;border:0;border-radius:16px;padding:22px 24px 20px}
.feedback-dialog-head{display:flex;align-items:flex-start;justify-content:space-between;gap:14px;margin-bottom:4px}
.feedback-dialog-head h2{margin:0;color:var(--ink);font-size:18px;font-weight:760;line-height:1.35}
.feedback-dialog-close{display:inline-flex;align-items:center;justify-content:center;width:30px;height:30px;padding:0;border:1px solid var(--hairline);border-radius:999px;background:var(--canvas);color:var(--steel);font-size:18px;font-weight:700;line-height:1}
.feedback-dialog-close:hover{border-color:var(--ink);color:var(--ink);background:var(--surface-soft)}
.feedback-dialog-actions{display:flex;align-items:center;justify-content:flex-end;gap:10px;margin:16px 0 0}
.feedback-dialog-cancel{display:inline-flex;align-items:center;justify-content:center;height:36px;padding:0 16px;border:1px solid var(--hairline);border-radius:999px;background:var(--canvas);color:var(--ink);font-size:14px;font-weight:600;line-height:1}
.muted{color:var(--steel)}
@media (max-width:900px){.attempt-head{align-items:flex-start;flex-direction:column}.attempt-title{flex-wrap:wrap}.attempt-side{align-items:flex-start;flex-direction:column;gap:6px}.attempt-main,.attempt-meta{white-space:normal}.attempt-time{text-align:left}.attempt-copy{-webkit-line-clamp:3}.review-grid{grid-template-columns:1fr}.attempt-detail-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media (max-width:760px){.shell,main{padding-left:12px;padding-right:12px}.topbar{align-items:flex-start;flex-direction:column;padding:14px 0}.grid{grid-template-columns:1fr}th,td{padding:10px 12px}.attempt-foot{align-items:flex-start;flex-direction:column}.attempt-conversation-banner{align-items:flex-start;flex-direction:column}.attempt-detail-grid{grid-template-columns:1fr}.todo-detail-fields{grid-template-columns:1fr}.todo-followup-time{margin-left:0}.tasks-table th{top:0}.log-head{grid-template-columns:1fr}.log-time{text-align:left}.log-body{grid-template-columns:1fr}.history-chart{height:220px}.table-toolbar{grid-template-columns:1fr}.table-toolbar-left{flex-wrap:wrap}.table-toolbar-search{flex:1 1 260px;min-width:0}.table-toolbar-center{justify-content:flex-start}.table-toolbar-right{justify-content:flex-start}}
"""

FAVICON_HREF = (
    "data:image/svg+xml,"
    "%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E"
    "%3Crect width='64' height='64' rx='14' fill='%230a0a0a'/%3E"
    "%3Crect x='8' y='42' width='48' height='10' rx='5' fill='%2300d4a4'/%3E"
    "%3C/svg%3E"
)
CONTEXT_ONLY_TOOLTIP = (
    "未调用工具；本次回答仅基于对话上下文生成。"
)
NO_AUDIT_DOCUMENTS_TOOLTIP = (
    "未附加审计材料；本次回答没有使用文档证据。"
)
NO_AUDIT_CONTEXT_TOOLTIP = (
    "未附加审计材料或工具事件；本次回答仅基于对话上下文生成。"
)
NO_CODEX_SESSION_TOOLTIP = (
    "这条记录没有关联执行会话，可直接查看页面中的判断依据和审计字段。"
)
_BROWSER_NOTIFICATION_SUBSCRIBERS: set[asyncio.Queue[dict[str, str]]] = set()
_BROWSER_NOTIFICATION_HISTORY: deque[dict[str, str]] = deque(maxlen=20)
_BROWSER_NOTIFICATION_SEQUENCE = count(1)
_DINGTALK_BRIDGE_STATUS: deque[dict[str, str]] = deque(maxlen=20)
INFINITE_LIST_PAGE_SIZE = 100
DEFAULT_ATTEMPT_LIST_LIMIT = INFINITE_LIST_PAGE_SIZE
ATTEMPT_LIST_LIMIT_OPTIONS = (INFINITE_LIST_PAGE_SIZE,)
HISTORY_TYPE_FILTERS = ("sent", "reacted", "skipped", "failed")
TASK_PAGE_SIZE_OPTIONS = (INFINITE_LIST_PAGE_SIZE,)
DEFAULT_TASK_PAGE_SIZE = INFINITE_LIST_PAGE_SIZE
LOG_PAGE_SIZE_OPTIONS = (INFINITE_LIST_PAGE_SIZE,)
DEFAULT_ERROR_LIST_LIMIT = INFINITE_LIST_PAGE_SIZE
HISTORY_CHART_HOURS = 24
HISTORY_CHART_COLORS = {
    "已发送": "#00b48a",
    "已跳过": "#a8a8aa",
    "处理中": "#3772cf",
    "失败": "#d45656",
    "预演": "#c37d0d",
    "已接受": "#00b48a",
    "暂定": "#c37d0d",
    "已拒绝": "#d45656",
    "已通过": "#00b48a",
    "已评论": "#3772cf",
    "已退回": "#c37d0d",
}

LUCIDE_ICON_PATHS = {
    "arrow-left": '<path d="m12 19-7-7 7-7"/><path d="M19 12H5"/>',
    "calendar-check": (
        '<path d="M8 2v4"/><path d="M16 2v4"/>'
        '<rect width="18" height="18" x="3" y="4" rx="2"/>'
        '<path d="M3 10h18"/><path d="m9 16 2 2 4-4"/>'
    ),
    "calendar-clock": (
        '<path d="M8 2v4"/><path d="M16 2v4"/>'
        '<rect width="18" height="18" x="3" y="4" rx="2"/>'
        '<path d="M3 10h18"/><path d="M17 14v3l2 1"/>'
        '<path d="M14 16a5 5 0 1 0 10 0 5 5 0 1 0-10 0"/>'
    ),
    "calendar-x": (
        '<path d="M8 2v4"/><path d="M16 2v4"/>'
        '<rect width="18" height="18" x="3" y="4" rx="2"/>'
        '<path d="M3 10h18"/><path d="m10 14 4 4"/>'
        '<path d="m14 14-4 4"/>'
    ),
    "check": '<path d="M20 6 9 17l-5-5"/>',
    "circle-alert": (
        '<circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="8" y2="12"/>'
        '<line x1="12" x2="12.01" y1="16" y2="16"/>'
    ),
    "clipboard-check": (
        '<rect width="8" height="4" x="8" y="2" rx="1" ry="1"/>'
        '<path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"/>'
        '<path d="m9 14 2 2 4-4"/>'
    ),
    "clipboard-pen-line": (
        '<rect width="8" height="4" x="8" y="2" rx="1"/>'
        '<path d="M8 4H6a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h6"/>'
        '<path d="M16 4h2a2 2 0 0 1 2 2v6"/>'
        '<path d="M21.378 16.626a1 1 0 0 0-3.004-3.004l-4.01 4.012a2 2 0 0 0-.506.854l-.837 2.87a.5.5 0 0 0 .62.62l2.87-.837a2 2 0 0 0 .854-.506z"/>'
        '<path d="M8 18h1"/>'
    ),
    "message-circle": '<path d="M7.9 20A9 9 0 1 0 4 16.1L2 22Z"/>',
    "smile": (
        '<circle cx="12" cy="12" r="10"/>'
        '<path d="M8 14s1.5 2 4 2 4-2 4-2"/>'
        '<line x1="9" x2="9.01" y1="9" y2="9"/>'
        '<line x1="15" x2="15.01" y1="9" y2="9"/>'
    ),
}


def _lucide_icon(name: str) -> str:
    path = LUCIDE_ICON_PATHS.get(name)
    if not path:
        return ""
    return (
        '<svg class="lucide-icon" data-lucide-icon="'
        f'{escape(name, quote=True)}" xmlns="http://www.w3.org/2000/svg" '
        'viewBox="0 0 24 24" aria-hidden="true" focusable="false">'
        f"{path}</svg>"
    )


def _status_action_pill(label: str, state: str, icon_name: str) -> str:
    return (
        f"<span class=\"pill status-action {_action_state_class(state)}\">"
        f"{_lucide_icon(icon_name)}<span>{escape(label)}</span></span>"
    )


class _TutorialStep(TypedDict):
    phase: str
    title: str
    description: str
    checks: list[str]
    commands: list[str]
    links: list[tuple[str, str]]


def render_page(
    title: str,
    body: str,
    *,
    auto_refresh: bool = False,
    active_nav: str | None = None,
    user_feedback_pending_count: int | None = None,
    head_extra: str = "",
    show_nav: bool = True,
    show_header: bool = True,
    main_class: str = "",
) -> str:
    refresh_meta = (
        "<meta http-equiv=\"refresh\" content=\"15\">" if auto_refresh else ""
    )
    nav_html = _top_nav(active_nav, user_feedback_pending_count) if show_nav else ""
    header_html = (
        "<header><div class=\"shell topbar\"><a class=\"brand brand-home\" href=\"/\" aria-label=\"返回处理记录\">"
        "<div class=\"brand-mark\"></div><div>"
        f"<h1>{escape(title)}</h1><div class=\"eyebrow\">一人 CEO 工作台</div>"
        "</div></a>"
        f"{nav_html}"
        "</div></header>"
        if show_header
        else ""
    )
    return (
        "<!doctype html><html><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
        f"{refresh_meta}"
        f"<title>{escape(title)}</title>"
        f"<link rel=\"icon\" href=\"{FAVICON_HREF}\">"
        f"<style>{CSS}</style>{head_extra}</head><body>"
        f"{header_html}<main{f' class=\"{escape(main_class)}\"' if main_class else ''}>"
        f"{body}</main>{_custom_select_script()}{_browser_notification_client_script()}</body></html>"
    )


def _custom_select_script() -> str:
    return """
<script data-custom-select>
(() => {
  const chevronSvg = '<svg class="custom-select-chevron" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"><path d="m6 9 6 6 6-6"/></svg>';
  const checkSvg = '<svg class="custom-select-check" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"><path d="M20 6 9 17l-5-5"/></svg>';
  let openRoot = null;
  let openMenu = null;

  const selectedText = (select) => {
    const option = select.options[select.selectedIndex];
    return option ? option.textContent.trim() : "";
  };
  const closeMenu = () => {
    if (openRoot) {
      openRoot.classList.remove("open");
      const trigger = openRoot.querySelector(".custom-select-trigger");
      if (trigger) {
        trigger.setAttribute("aria-expanded", "false");
      }
    }
    if (openMenu) {
      openMenu.remove();
    }
    openRoot = null;
    openMenu = null;
  };
  const positionMenu = () => {
    if (!openRoot || !openMenu) {
      return;
    }
    const trigger = openRoot.querySelector(".custom-select-trigger");
    if (!trigger) {
      return;
    }
    const rect = trigger.getBoundingClientRect();
    const margin = 6;
    const menuWidth = Math.max(rect.width, 132);
    openMenu.style.minWidth = `${menuWidth}px`;
    openMenu.style.left = `${Math.max(8, Math.min(rect.left, window.innerWidth - menuWidth - 8))}px`;
    openMenu.style.top = `${Math.min(rect.bottom + margin, window.innerHeight - 40)}px`;
  };
  const syncLabel = (select) => {
    const root = select.nextElementSibling;
    if (!root || !root.classList || !root.classList.contains("custom-select")) {
      return;
    }
    const label = root.querySelector(".custom-select-label");
    if (label) {
      label.textContent = selectedText(select);
    }
  };
  const chooseOption = (select, index) => {
    const option = select.options[index];
    if (!option || option.disabled) {
      return;
    }
    select.selectedIndex = index;
    syncLabel(select);
    closeMenu();
    select.dispatchEvent(new Event("input", {bubbles: true}));
    select.dispatchEvent(new Event("change", {bubbles: true}));
  };
  const openSelect = (select, root) => {
    if (openRoot === root) {
      closeMenu();
      return;
    }
    closeMenu();
    openRoot = root;
    root.classList.add("open");
    const trigger = root.querySelector(".custom-select-trigger");
    if (trigger) {
      trigger.setAttribute("aria-expanded", "true");
    }
    const menu = document.createElement("div");
    menu.className = "custom-select-menu";
    menu.setAttribute("role", "listbox");
    menu.setAttribute("aria-label", select.getAttribute("aria-label") || select.name || "Select");
    Array.from(select.options).forEach((option, index) => {
      const item = document.createElement("button");
      item.type = "button";
      item.className = "custom-select-option";
      item.setAttribute("role", "option");
      item.setAttribute("aria-selected", index === select.selectedIndex ? "true" : "false");
      if (option.disabled) {
        item.setAttribute("aria-disabled", "true");
      }
      const text = document.createElement("span");
      text.textContent = option.textContent.trim();
      item.appendChild(text);
      item.insertAdjacentHTML("beforeend", checkSvg);
      item.addEventListener("click", () => chooseOption(select, index));
      menu.appendChild(item);
    });
    document.body.appendChild(menu);
    openMenu = menu;
    positionMenu();
    const selected = menu.querySelector('[aria-selected="true"]');
    if (selected) {
      selected.focus({preventScroll: true});
    }
  };
  const enhanceSelect = (select) => {
    if (!(select instanceof HTMLSelectElement) || select.dataset.customSelectEnhanced === "1") {
      return;
    }
    const measuredWidth = Math.max(select.getBoundingClientRect().width || select.offsetWidth || 132, 96);
    select.dataset.customSelectEnhanced = "1";
    const root = document.createElement("span");
    root.className = "custom-select";
    if (select.classList.contains("table-type-select")) {
      root.classList.add("custom-table-type-select");
    }
    if (select.classList.contains("table-page-size")) {
      root.classList.add("custom-table-page-size");
    }
    root.style.width = `${measuredWidth}px`;
    const trigger = document.createElement("button");
    trigger.type = "button";
    trigger.className = "custom-select-trigger";
    trigger.setAttribute("aria-haspopup", "listbox");
    trigger.setAttribute("aria-expanded", "false");
    trigger.setAttribute("aria-label", select.getAttribute("aria-label") || select.name || "Select");
    trigger.innerHTML = `<span class="custom-select-label"></span>${chevronSvg}`;
    root.appendChild(trigger);
    select.insertAdjacentElement("afterend", root);
    syncLabel(select);
    trigger.addEventListener("click", () => openSelect(select, root));
    trigger.addEventListener("keydown", (event) => {
      if (event.key === "ArrowDown" || event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        openSelect(select, root);
      }
    });
    select.addEventListener("change", () => syncLabel(select));
  };
  const enhanceAll = (root = document) => {
    root.querySelectorAll("select").forEach(enhanceSelect);
  };
  document.addEventListener("click", (event) => {
    if (!openRoot) {
      return;
    }
    if (openRoot.contains(event.target) || (openMenu && openMenu.contains(event.target))) {
      return;
    }
    closeMenu();
  });
  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") {
      return;
    }
    closeMenu();
  });
  window.addEventListener("resize", positionMenu);
  window.addEventListener("scroll", positionMenu, {passive: true});
  enhanceAll();
  const observer = new MutationObserver((mutations) => {
    mutations.forEach((mutation) => {
      mutation.addedNodes.forEach((node) => {
        if (node.nodeType !== 1) {
          return;
        }
        if (node.matches && node.matches("select")) {
          enhanceSelect(node);
        }
        if (node.querySelectorAll) {
          enhanceAll(node);
        }
      });
    });
  });
  observer.observe(document.documentElement, {childList: true, subtree: true});
})();
</script>
"""


def render_browser_notifications_page() -> str:
    body = """
<section class="card">
<h2>Chrome 通知</h2>
<p class="muted">打开这个页面并允许通知后，CEO 服务会优先通过 Chrome 弹出通知。点击通知会打开对应的钉钉会话。</p>
<div class="notification-panel">
  <button type="button" id="enable-notifications">允许 Chrome 通知</button>
  <span class="pill" id="notification-state">checking</span>
</div>
<pre class="notification-log" id="notification-log">等待连接...</pre>
</section>
"""
    return render_page("浏览器通知", body, active_nav="notifications")


def _expand_configured_path(value: str) -> Path:
    return Path(os.path.expandvars(value)).expanduser()


def _configured_worker_db_path() -> Path:
    configured_db_path = os.environ.get("CEO_WORKER_DB", "").strip()
    if not configured_db_path:
        return Path("data/auto-reply.sqlite3")
    return _expand_configured_path(configured_db_path)


def render_tutorial_page(*, store: AutoReplyStore | None = None) -> str:
    if store is None:
        store = AutoReplyStore(_configured_worker_db_path())
    status = build_wizard_status(store)
    steps_html = "".join(_setup_wizard_step_html(step) for step in status.steps)
    body = (
        "<section class=\"card tutorial-intro\">"
        "<h2>初始化向导</h2>"
        "<p class=\"muted\">"
        "这里用于检查和配置本地一人 CEO 服务。每一步只有在系统验证通过后才会标记完成。"
        "</p>"
        "</section>"
        "<section class=\"card\">"
        "<div class=\"card-head\">"
        "<h2>配置步骤</h2>"
        "<div class=\"tutorial-links\">"
        "<a class=\"tutorial-link\" href=\"/config?tab=system\">系统参数</a>"
        "<a class=\"tutorial-link\" href=\"/tasks\">任务</a>"
        "<a class=\"tutorial-link\" href=\"/logs\">运行日志</a>"
        "</div>"
        "</div>"
        f"<ol class=\"tutorial-steps setup-wizard-steps\">{steps_html}</ol>"
        "</section>"
    )
    return render_page("初始化向导", body, active_nav="tutorial")


def _setup_wizard_step_html(step: SetupStepStatus) -> str:
    action_html = "".join(
        "<form method=\"post\" action=\"/tutorial/"
        f"{'check' if action.kind == 'check' else 'run' if action.kind == 'run' else 'confirm'}"
        f"/{escape(action.id if action.kind == 'run' else step.step_id)}\">"
        f"<button type=\"submit\" data-action-id=\"{escape(action.id)}\">"
        f"{escape(action.label)}</button>"
        "</form>"
        for action in step.available_actions
        if action.kind != "confirm" or step.manual_confirmation_allowed
    )
    evidence_html = "".join(
        "<li>"
        f"<code>{escape(str(key))}</code>: {escape(str(value))}"
        "</li>"
        for key, value in step.evidence.items()
    )
    evidence_list = (
        f"<ul class=\"tutorial-list\">{evidence_html}</ul>"
        if evidence_html
        else ""
    )
    return (
        "<li class=\"tutorial-step setup-wizard-step\">"
        "<div class=\"tutorial-step-number\" aria-hidden=\"true\"></div>"
        "<div class=\"tutorial-step-body\">"
        "<div class=\"tutorial-step-head\">"
        f"<h3>{escape(step.title)}</h3>"
        f"<span class=\"setup-step-status setup-status-{escape(step.status)}\">"
        f"{escape(step.status)}</span>"
        "</div>"
        f"<p>{escape(step.summary or '尚未检查。')}</p>"
        f"{evidence_list}"
        f"<div class=\"tutorial-links\">{action_html}</div>"
        "</div>"
        "</li>"
    )


def _tutorial_step_html(step: _TutorialStep) -> str:
    checks_html = _tutorial_list_html(step["checks"], class_name="tutorial-list")
    commands_html = _tutorial_command_list_html(step["commands"])
    links_html = "".join(
        f"<a class=\"tutorial-link\" href=\"{escape(href)}\">{escape(label)}</a>"
        for label, href in step["links"]
    )
    return (
        "<li class=\"tutorial-step\">"
        "<div class=\"tutorial-step-number\" aria-hidden=\"true\"></div>"
        "<div class=\"tutorial-step-body\">"
        "<div class=\"tutorial-step-head\">"
        f"<h3>{escape(str(step['title']))}</h3>"
        f"<span class=\"tutorial-phase\">{escape(str(step['phase']))}</span>"
        "</div>"
        f"<p>{escape(str(step['description']))}</p>"
        "<div class=\"tutorial-lists\">"
        f"{checks_html}"
        f"{commands_html}"
        "</div>"
        f"<div class=\"tutorial-links\">{links_html}</div>"
        "</div>"
        "</li>"
    )


def _tutorial_list_html(items: list[str], *, class_name: str) -> str:
    return (
        f"<ul class=\"{escape(class_name)}\">"
        + "".join(f"<li>{escape(str(item))}</li>" for item in items)
        + "</ul>"
    )


def _tutorial_command_list_html(commands: list[str]) -> str:
    return (
        "<div class=\"tutorial-command-list\">"
        + "".join(f"<code>{escape(str(command))}</code>" for command in commands)
        + "</div>"
    )


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def confirm_setup_step(
    step_id: str,
    *,
    store: AutoReplyStore,
    confirmed_by: str,
    evidence: dict[str, str],
) -> SetupWizardEvent:
    try:
        definition = get_step_definition(step_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Unknown setup step") from exc
    if not any(action.kind == "confirm" for action in definition.actions):
        raise HTTPException(
            status_code=409,
            detail=f"{definition.title} does not allow manual confirmation.",
        )
    if not confirmed_by.strip():
        raise HTTPException(status_code=400, detail="confirmed_by is required.")
    summary = f"Manually confirmed {definition.title}."
    store.upsert_setup_wizard_step(
        step_id=definition.id,
        status="done",
        summary=summary,
        manual_confirmed_by=confirmed_by,
    )
    return SetupWizardEvent(
        step_id=definition.id,
        action_id=f"confirm_{definition.id}",
        status="done",
        summary=summary,
        evidence=evidence,
    )


def _setup_status_map(store: AutoReplyStore) -> dict[str, SetupStepStatus]:
    return {step.step_id: step for step in build_wizard_status(store).steps}


def _require_available_setup_action(
    store: AutoReplyStore,
    action_id: str,
    *,
    kind: str,
):
    try:
        definition = get_action_definition(action_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Unknown setup action") from exc
    if definition.kind != kind:
        raise HTTPException(status_code=400, detail="Wrong setup action type.")
    step_status = _setup_status_map(store).get(definition.step_id)
    if step_status is None:
        raise HTTPException(status_code=404, detail="Unknown setup step")
    if not any(action.id == action_id for action in step_status.available_actions):
        raise HTTPException(
            status_code=409,
            detail=f"{step_status.title} is not ready for this action.",
        )
    return definition


def _wants_setup_redirect(request: Request) -> bool:
    content_type = request.headers.get("content-type", "")
    return (
        "application/x-www-form-urlencoded" in content_type
        or "multipart/form-data" in content_type
    )


def _setup_action_response(request: Request, payload) -> Response:
    if _wants_setup_redirect(request):
        return RedirectResponse("/tutorial", status_code=303)
    return JSONResponse(payload.model_dump())


def _tutorial_steps() -> list[_TutorialStep]:
    return [
        {
            "phase": "Phase 0",
            "title": "收集交互参数",
            "description": "先确认本机路径和身份参数，再改配置；不知道的值先检查机器，只有授权、扫码、策略选择才打断用户。",
            "checks": [
                "Repository path: ~/Documents/Projects/ceo-agent-service",
                "Workspace path: ~/Documents/memory",
                "Principal display name, mention aliases, signature, handoff acknowledgement",
                "Memory Connector MCP URL and DingTalk KB workspace are optional",
            ],
            "commands": [
                "sed -n '1,240p' ~/.agents/AGENT.md",
                "git status --short --branch",
            ],
            "links": [("运行说明", "/config"), ("系统参数", "/config?tab=system")],
        },
        {
            "phase": "Phase 1",
            "title": "准备本地依赖和 CLI",
            "description": "确认 Python 环境、dws CLI、本地执行 CLI 和仓库依赖可用；HOME 必须是真实用户目录，不能指向项目目录。",
            "checks": [
                "Python 3.11+ and editable package install",
                "dws auth status and dws doctor pass under the real user account",
                "本地执行 CLI 可通过本机运行时启动自动处理任务",
                "正式发送前先使用预演模式检查工作台结果",
            ],
            "commands": [
                "python3 -m venv .venv",
                ".venv/bin/pip install -e '.[dev]'",
                "dws auth status",
                "local-runner --version",
            ],
            "links": [("规则与记忆", "/config"), ("运行日志", "/logs")],
        },
        {
            "phase": "Phase 2",
            "title": "配置 MCP 和基础环境",
            "description": "按 README 配置 Memory Connector MCP、.env、workspace、SQLite 和 corpus 目录；MCP 身份使用已安装 Authorization header，不单独填写 user_id。",
            "checks": [
                ".env comes from .env.example and stays uncommitted",
                "CEO_WORKSPACE, CEO_WORKER_DB, CEO_CORPUS_DIR point at local paths",
                "Memory Connector MCP is optional but must use the authenticated OAuth identity",
                "CEO_NOT_SEND_MESSAGE=1 or CEO_DRY_RUN=1 remains enabled",
            ],
            "commands": [
                "cp .env.example .env",
                ".venv/bin/ceo-agent setup-memory-connector --memory-url '<memory-mcp-url>'",
                "mkdir -p data/corpus \"$HOME/Documents/memory\"",
            ],
            "links": [("系统参数", "/config?tab=system")],
        },
        {
            "phase": "Phase 4",
            "title": "准备本地数据和风格语料",
            "description": "把 AI 听记、SOP、招聘、战略和 Thinking 材料放在 CEO_WORKSPACE 或其他忽略路径，不把私有数据放进 Git。",
            "checks": [
                "本地材料包含 AI 听记、OA、战略、招聘和 Thinking 等工作上下文",
                "build-corpus 读取本地会议纪要并生成风格语料",
                "collect-corpus 通过当前 dws 身份追加近期钉钉已发送样本",
                "data/corpus/style_corpus.csv 是本地运行数据，不进入源码",
            ],
            "commands": [
                ".venv/bin/ceo-agent build-corpus --workspace \"$HOME/Documents/memory\" --corpus-dir ./data/corpus",
                ".venv/bin/ceo-agent collect-corpus --workspace \"$HOME/Documents/memory\" --corpus-dir ./data/corpus",
            ],
            "links": [("任务", "/tasks")],
        },
        {
            "phase": "Phase 5",
            "title": "生成并复核工作画像蒸馏",
            "description": "build-work-profile 生成证据索引和初版 profile；Nvwa 只在准备/复核阶段使用，运行时只读取 data/work-profile/work_profile.md。",
            "checks": [
                "预期产物：data/work-profile/work_profile.md、data/profile-evidence/evidence_index.jsonl、data/corpus/style_corpus.csv",
                "Nvwa 复核只改写 data/work-profile/work_profile.md",
                "Profile 不应包含原始隐私片段、绝对路径、token、session id 或钉钉缓存内容",
                "运行时通过 work_profile_instruction() 读取画像",
            ],
            "commands": [
                ".venv/bin/ceo-agent build-work-profile --workspace \"$HOME/Documents/memory\" --corpus-dir ./data/corpus",
                ".venv/bin/pytest tests/test_work_profile.py tests/test_prompt.py tests/test_worker.py::test_consumer_codex_command_embeds_work_profile_content -q",
            ],
            "links": [("规则与记忆", "/config"), ("运行日志", "/logs")],
        },
        {
            "phase": "Phase 6",
            "title": "验证权限和 dry-run 审计",
            "description": "先做只读权限探测，再运行一次 dry-run；审计页必须能解释路由、证据、错误和未发送状态。",
            "checks": [
                "dws 可以读取部署所需的未读会话、文档、AI 表格、通讯录、日历、OA 和 AI 听记",
                "工作台可在 127.0.0.1:8765 打开",
                "预演模式没有意外真实发送",
                "没有未处理的失败或处理中积压",
            ],
            "commands": [
                ".venv/bin/ceo-agent probe-dws",
                ".venv/bin/python -m app.cli audit-web --reload --host 127.0.0.1 --port 8765",
                "CEO_NOT_SEND_MESSAGE=1 .venv/bin/ceo-agent run-once --not-send-message",
            ],
            "links": [("处理记录", "/"), ("运行日志", "/logs"), ("任务", "/tasks")],
        },
        {
            "phase": "Phase 8",
            "title": "安装 launchd，最后再决定 live send",
            "description": "launchd 只在 dry-run 行为被审阅后安装；真实发送需要明确设置 CEO_NOT_SEND_MESSAGE=0 和 CEO_LIVE_SEND_BLOCKERS_ACCEPTED=1。",
            "checks": [
                "安装前检查 launchd/com.ceo-agent-service.main.plist",
                "launchctl print 确认 com.ceo-agent-service.main 正在运行",
                "按会话、别名、动作、OA/日历/跟进边界复核真实发送范围",
                "安装成功不代表默认接受 CEO_LIVE_SEND_BLOCKERS_ACCEPTED=1",
            ],
            "commands": [
                "scripts/install-auto-reply-agents.sh",
                "launchctl print gui/$(id -u)/com.ceo-agent-service.main | sed -n '1,80p'",
                "CEO_NOT_SEND_MESSAGE=0 CEO_LIVE_SEND_BLOCKERS_ACCEPTED=1 .venv/bin/ceo-agent send-attempt --attempt-id <reviewed-attempt-id>",
            ],
            "links": [("处理记录", "/"), ("运行日志", "/logs")],
        },
    ]


def _browser_notification_client_script() -> str:
    return """
<script>
(() => {
  const lockKey = "ceo-agent-service-notification-leader";
  const lockTtlMs = 5000;
  const heartbeatMs = 2000;
  const tabId = crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}-${Math.random()}`;
  const stateEl = document.getElementById("notification-state");
  const logEl = document.getElementById("notification-log");
  const enableButton = document.getElementById("enable-notifications");
  let events = null;
  let serviceWorkerReady = null;

  function logLine(text) {
    if (!logEl) {
      return;
    }
    const timestamp = new Date().toLocaleTimeString();
    logEl.textContent = `[${timestamp}] ${text}\n` + logEl.textContent;
  }

  function setState(text) {
    if (stateEl) {
      stateEl.textContent = text;
    }
  }

  function canNotify() {
    return "Notification" in window && Notification.permission === "granted";
  }

  function ensureServiceWorker() {
    if (!("serviceWorker" in navigator)) {
      return Promise.resolve(null);
    }
    if (!serviceWorkerReady) {
      serviceWorkerReady = navigator.serviceWorker
        .register("/notification-service-worker.js")
        .then(() => navigator.serviceWorker.ready)
        .catch((error) => {
          logLine(`service worker failed: ${error}`);
          return null;
        });
    }
    return serviceWorkerReady;
  }

  function readLock() {
    try {
      return JSON.parse(localStorage.getItem(lockKey) || "null");
    } catch (error) {
      return null;
    }
  }

  function writeLock() {
    localStorage.setItem(lockKey, JSON.stringify({ id: tabId, ts: Date.now() }));
  }

  function ownsFreshLock() {
    const lock = readLock();
    return lock && lock.id === tabId && Date.now() - Number(lock.ts || 0) < lockTtlMs;
  }

  function releaseLock() {
    if (ownsFreshLock()) {
      localStorage.removeItem(lockKey);
    }
  }

  async function showBrowserNotification(payload) {
    logLine(`${payload.title}: ${payload.message}`);
    if (!canNotify()) {
      return;
    }
    const options = {
      body: payload.message,
      tag: payload.id,
      renotify: true,
      data: { url: payload.url || "", detailUrl: payload.detail_url || "" },
    };
    const registration = await ensureServiceWorker();
    if (!registration) {
      logLine("notification skipped: service worker unavailable");
      return;
    }
    await registration.showNotification(payload.title, options);
  }

  function stopEvents() {
    if (events) {
      events.close();
      events = null;
    }
  }

  function startEvents() {
    if (events) {
      return;
    }
    events = new EventSource("/notifications/events");
    events.onopen = () => logLine("connected to 8765 notification stream");
    events.onerror = () => logLine("notification stream reconnecting");
    events.onmessage = (event) => {
      showBrowserNotification(JSON.parse(event.data));
    };
  }

  function refreshPermission() {
    if (!("Notification" in window)) {
      setState("not supported");
      if (enableButton) {
        enableButton.disabled = true;
      }
      return;
    }
    setState(Notification.permission);
  }

  function electLeader() {
    refreshPermission();
    if (!canNotify()) {
      releaseLock();
      stopEvents();
      return;
    }
    const lock = readLock();
    const lockIsStale = !lock || Date.now() - Number(lock.ts || 0) > lockTtlMs;
    if (lockIsStale || lock.id === tabId) {
      writeLock();
      ensureServiceWorker();
      startEvents();
      setState("granted connected");
      return;
    }
    stopEvents();
    setState("granted standby");
  }

  async function requestNotificationPermission() {
    if (!("Notification" in window)) {
      refreshPermission();
      return;
    }
    const permission = await Notification.requestPermission();
    logLine(`permission: ${permission}`);
    if (permission === "granted") {
      await ensureServiceWorker();
    }
    electLeader();
  }

  if (enableButton) {
    enableButton.addEventListener("click", requestNotificationPermission);
  }
  if ("serviceWorker" in navigator) {
    navigator.serviceWorker.addEventListener("message", (event) => {
      const payload = event.data || {};
      if (payload.type !== "ceo-agent-service:navigate" || !payload.url) {
        return;
      }
      const target = new URL(payload.url, window.location.origin);
      if (target.origin !== window.location.origin) {
        return;
      }
      const targetPath = `${target.pathname}${target.search}${target.hash}`;
      const currentPath = `${window.location.pathname}${window.location.search}${window.location.hash}`;
      if (targetPath !== currentPath) {
        window.location.assign(targetPath);
      }
    });
  }
  window.addEventListener("storage", (event) => {
    if (event.key === lockKey) {
      electLeader();
    }
  });
  window.addEventListener("beforeunload", () => {
    releaseLock();
    stopEvents();
  });
  setInterval(electLeader, heartbeatMs);
  electLeader();
})();
</script>
"""


def _notification_service_worker_script() -> str:
    return """
self.addEventListener("install", (event) => {
  event.waitUntil(self.skipWaiting());
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  event.waitUntil(handleNotificationClick(event.notification.data || {}));
});

async function handleNotificationClick(data) {
  if (data.url) {
    try {
      await fetch(data.url, {
        method: "GET",
        headers: { "Accept": "application/json" },
      });
    } catch (error) {
      // The backend bridge is best-effort; do not open a fallback browser tab.
    }
  }
  const windows = await self.clients.matchAll({
    type: "window",
    includeUncontrolled: true,
  });
  for (const client of windows) {
    try {
      if (new URL(client.url).origin === self.location.origin && client.focus) {
        await client.focus();
        if (data.detailUrl && client.postMessage) {
          client.postMessage({
            type: "ceo-agent-service:navigate",
            url: data.detailUrl,
          });
        }
        return;
      }
    } catch (error) {
      // Ignore malformed client URLs.
    }
  }
}
"""


def _browser_notification_event(
    *,
    title: str,
    message: str,
    url: str,
) -> dict[str, str]:
    return {
        "id": f"ceo-agent-service-{next(_BROWSER_NOTIFICATION_SEQUENCE)}",
        "title": title,
        "message": message,
        "url": url,
        "detail_url": _notification_detail_url(url),
    }


def _notification_detail_url(url: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    attempt_ids = query.get("attempt_id", [])
    if not attempt_ids:
        return ""
    try:
        attempt_id = int(attempt_ids[0])
    except ValueError:
        return ""
    if attempt_id <= 0:
        return ""
    return f"/attempts/{attempt_id}"


def _dingtalk_conversation_url(cid: str) -> str:
    return (
        "dingtalk://dingtalkclient/page/conversation"
        f"?cid={quote(cid.strip(), safe='')}"
    )


def _dingtalk_pc_slide_link_url(link: str) -> str:
    return (
        "dingtalk://dingtalkclient/page/link"
        f"?url={quote(link, safe='')}&pc_slide=true"
    )


def _dingtalk_url_from_bridge_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.path != "/open-dingtalk":
        return ""
    query = parse_qs(parsed.query)
    conversation_id = (query.get("conversation_id") or [""])[0].strip()
    if conversation_id:
        return _dingtalk_pc_slide_link_url(
            f"{parsed.scheme}://{parsed.netloc}/dingtalk/open-chat-bridge"
            f"?conversation_id={quote(conversation_id, safe='')}"
        )
    cid = (query.get("cid") or [""])[0].strip()
    if not cid:
        return ""
    return _dingtalk_conversation_url(cid)


def render_dingtalk_open_chat_bridge(open_conversation_id: str) -> str:
    escaped_conversation_id = json.dumps(open_conversation_id)
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>打开钉钉会话</title>
  <style>
    body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;margin:0;padding:28px;background:#fff;color:#111;line-height:1.5}}
    .card{{max-width:520px;margin:12vh auto 0;border:1px solid #e5e5e5;border-radius:12px;padding:22px;background:#fafafa}}
    h1{{margin:0 0 10px;font-size:18px}}
    p{{margin:8px 0;color:#555}}
    code{{word-break:break-all;background:#eee;border-radius:6px;padding:2px 5px}}
  </style>
  <script src="https://g.alicdn.com/dingding/dingtalk-jsapi/3.0.25/dingtalk.open.js"></script>
</head>
<body>
  <section class="card">
    <h1>正在打开钉钉会话</h1>
    <p id="status">等待钉钉 JSAPI...</p>
    <p><code>{escape(open_conversation_id)}</code></p>
  </section>
  <script>
    const openConversationId = {escaped_conversation_id};
    const statusEl = document.getElementById("status");
    function report(stage, detail) {{
      const body = JSON.stringify({{
        conversation_id: openConversationId,
        stage,
        detail: detail || "",
      }});
      if (navigator.sendBeacon) {{
        navigator.sendBeacon("/dingtalk/bridge-status", body);
        return;
      }}
      fetch("/dingtalk/bridge-status", {{
        method: "POST",
        headers: {{ "Content-Type": "application/json" }},
        body,
      }}).catch(() => {{}});
    }}
    function setStatus(text) {{
      statusEl.textContent = text;
      report("status", text);
    }}
    function apiNames(dd) {{
      const root = Object.keys(dd || {{}}).sort().slice(0, 80);
      const chat = Object.keys((dd && dd.biz && dd.biz.chat) || {{}}).sort();
      return JSON.stringify({{ root, chat }});
    }}
    function closeBridgePageSoon() {{
      setTimeout(() => {{
        const dd = window.dd;
        const closeNavigation = dd && dd.biz && dd.biz.navigation && dd.biz.navigation.close;
        if (typeof closeNavigation === "function") {{
          report("close-navigation", "");
          closeNavigation({{}});
          return;
        }}
        if (dd && typeof dd.closePage === "function") {{
          report("close-page", "");
          dd.closePage({{}});
        }}
        window.close();
      }}, 600);
    }}
    async function openChat() {{
      const dd = window.dd;
      if (!dd) {{
        setStatus("钉钉 JSAPI 未加载。请确认本页是在钉钉客户端内打开。");
        return;
      }}
      report("dd-api-names", apiNames(dd));
      if (typeof dd.openChatByConversationId === "function") {{
        report("invoke", "openChatByConversationId");
        const ok = await new Promise((resolve) => {{
          let callbackSeen = false;
          const done = (result, text) => {{
            callbackSeen = true;
            setStatus(text);
            resolve(result);
          }};
          dd.openChatByConversationId({{
            openConversationId,
            success: () => done(true, "已通过当前会话 API 发起跳转。"),
            fail: (error) => done(false, `当前会话 API 跳转失败: ${{JSON.stringify(error)}}`),
            complete: () => {{}},
          }});
          setTimeout(() => {{
            if (!callbackSeen) {{
              report("callback-timeout", "openChatByConversationId");
              resolve(false);
            }}
          }}, 1200);
        }});
        if (ok) {{
          closeBridgePageSoon();
          return;
        }}
        return;
      }}
      setStatus("当前钉钉客户端没有可用的 openChatByConversationId 会话跳转能力。");
    }}
    function openWhenReady() {{
      report("loaded", navigator.userAgent);
      if (window.dd && typeof window.dd.ready === "function") {{
        let opened = false;
        const openOnce = () => {{
          if (opened) {{
            return;
          }}
          opened = true;
          openChat();
        }};
        window.dd.ready(() => {{
          report("dd-ready", "");
          openOnce();
        }});
        window.dd.error((error) => setStatus(`JSAPI 初始化失败: ${{JSON.stringify(error)}}`));
        setTimeout(() => {{
          report("dd-ready-timeout", "");
          openOnce();
        }}, 1000);
        return;
      }}
      setTimeout(openChat, 350);
    }}
    window.addEventListener("load", openWhenReady);
  </script>
</body>
</html>"""


def render_dingtalk_open_popup(*, cid: str = "", conversation_id: str = "") -> str:
    query: dict[str, str] = {}
    if conversation_id.strip():
        query["conversation_id"] = conversation_id.strip()
    if cid.strip():
        query["cid"] = cid.strip()
    open_url = "/open-dingtalk"
    if query:
        open_url = f"{open_url}?{urlencode(query)}"
    escaped_open_url = json.dumps(open_url)
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>打开钉钉消息</title>
  <style>
    body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;margin:0;padding:22px;background:#fff;color:#111;line-height:1.45}}
    .card{{border:1px solid #e5e5e5;border-radius:12px;padding:16px;background:#fafafa}}
    h1{{margin:0 0 8px;font-size:16px}}
    p{{margin:0;color:#555;font-size:13px}}
  </style>
</head>
<body>
  <section class="card">
    <h1>正在打开钉钉消息</h1>
    <p id="status">请稍候...</p>
  </section>
  <script>
    const statusEl = document.getElementById("status");
    function closeSoon() {{
      setTimeout(() => window.close(), 900);
    }}
    fetch({escaped_open_url}, {{cache: "no-store"}})
      .then((response) => response.json())
      .then((payload) => {{
        statusEl.textContent = payload && payload.ok ? "已发送打开请求，即将关闭。" : "打开请求失败，即将关闭。";
        closeSoon();
      }})
      .catch(() => {{
        statusEl.textContent = "打开请求失败，即将关闭。";
        closeSoon();
      }});
  </script>
</body>
</html>"""


def _publish_browser_notification(event: dict[str, str]) -> bool:
    _BROWSER_NOTIFICATION_HISTORY.append(event)
    subscribers = list(_BROWSER_NOTIFICATION_SUBSCRIBERS)
    for queue in subscribers:
        queue.put_nowait(event)
    return bool(subscribers)


def _browser_notification_event_stream() -> StreamingResponse:
    async def event_stream():
        queue: asyncio.Queue[dict[str, str]] = asyncio.Queue()
        _BROWSER_NOTIFICATION_SUBSCRIBERS.add(queue)
        try:
            yield ": connected\n\n"
            while True:
                event = await queue.get()
                data = json.dumps(event, ensure_ascii=False)
                yield f"data: {data}\n\n"
        finally:
            _BROWSER_NOTIFICATION_SUBSCRIBERS.discard(queue)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


def _top_nav(
    active_nav: str | None,
    user_feedback_pending_count: int | None = None,
) -> str:
    items = [
        ("history", "处理记录", "/"),
        ("tasks", "任务", "/tasks"),
        ("user-feedback", "用户反馈", "/user-feedback"),
        ("config", "规则与记忆", "/config"),
        ("codex", "执行会话", "/codex"),
        ("logs", "运行日志", "/logs"),
    ]
    item_html = "".join(
        _top_nav_item(
            key=key,
            label=label,
            href=href,
            active=key == active_nav,
            user_feedback_pending_count=user_feedback_pending_count,
        )
        for key, label, href in items
    )
    return f"<nav class=\"nav\">{item_html}</nav>"


def _top_nav_item(
    *,
    key: str,
    label: str,
    href: str,
    active: bool,
    user_feedback_pending_count: int | None,
) -> str:
    label_html = escape(label)
    if key == "user-feedback" and user_feedback_pending_count:
        badge_text = "99+" if user_feedback_pending_count > 99 else str(user_feedback_pending_count)
        label_html += f"<span class=\"nav-badge\">{escape(badge_text)}</span>"
    if active:
        return f"<span class=\"nav-item active\" aria-current=\"page\">{label_html}</span>"
    return f"<a class=\"nav-item\" href=\"{escape(href)}\">{label_html}</a>"


def render_config_page(
    *,
    active_tab: str = "recipe",
    saved: bool = False,
    dingtalk_reconnect: str = "",
    db_path: Path | None = None,
) -> str:
    tab_aliases = {
        "info": "recipe",
        "developer": "recipe",
        "user": "recipe",
        "dingtalk": "runtime",
        "reply": "runtime",
        "system": "system",
    }
    active_tab = tab_aliases.get(active_tab, active_tab)
    if active_tab == "recipe":
        content = _render_recipe_package(saved=saved)
    elif active_tab == "memory":
        content = _render_memory_view(db_path=db_path)
    elif active_tab == "runtime":
        content = _render_runtime_config_view(
            saved=saved,
            dingtalk_reconnect=dingtalk_reconnect,
            db_path=db_path,
        )
    elif active_tab == "system":
        content = _render_full_config(saved=saved, db_path=db_path)
    else:
        active_tab = "recipe"
        content = _render_recipe_package(saved=saved)
    body = f"{_config_tabs(active_tab)}{content}"
    pending_count = (
        AutoReplyStore(db_path).count_pending_user_feedback_items()
        if db_path is not None
        else None
    )
    return render_page(
        "规则与记忆",
        body,
        active_nav="config",
        user_feedback_pending_count=pending_count,
    )


def _render_rules_memory_overview(*, db_path: Path | None = None) -> str:
    store = AutoReplyStore(db_path) if db_path is not None else None
    reply_count = store.count_sent_replies() if store is not None else 0
    task_count = store.count_reply_tasks() if store is not None else 0
    feedback_count = (
        store.count_pending_user_feedback_items() if store is not None else 0
    )
    profile_ready = work_profile_path().exists()
    memory_ready = _codex_memory_connector_configured()
    return (
        "<section class=\"card config-hero\">"
        "<div class=\"config-hero-copy\">"
        "<h2>一人 CEO 的规则与记忆</h2>"
        "<p class=\"muted\">这里管理自动回复背后的规则、记忆、钉钉同步和运行策略。"
        "Recipe 决定系统如何判断和表达，Memory 提供长期背景和历史纠偏。</p>"
        "<div class=\"config-stage-list\">"
        f"{_config_stage('01', '钉钉消息进入', '连接钉钉后读取私聊、上下文、文档和组织信息。')}"
        f"{_config_stage('02', 'Recipe 判断', '原项目 Prompt、变量、工作画像和安全边界被打包成回复规则。')}"
        f"{_config_stage('03', 'Memory 补上下文', 'Memory MCP、历史纠偏、组织缓存和项目上下文一起提供长期事实。')}"
        f"{_config_stage('04', '回复与迭代', '自动回复、记录处理结果，并从用户反馈中沉淀后续规则调整。')}"
        "</div>"
        "</div>"
        "<div class=\"config-summary-grid\">"
        f"{_config_stat('已发送回复', str(reply_count))}"
        f"{_config_stat('任务记录', str(task_count))}"
        f"{_config_stat('待处理反馈', str(feedback_count))}"
        f"{_config_stat('Memory 状态', '可用' if memory_ready or profile_ready else '待配置')}"
        "</div>"
        "</section>"
        "<section class=\"card\">"
        "<h2>当前映射关系</h2>"
        "<div class=\"domain-grid\">"
        "<article class=\"domain-card\">"
        "<h3>Recipe 是什么</h3>"
        "<p>在这个工作台里，Recipe 等于原项目的“可执行规则包”。它不是单条提示词，"
        "而是身份、回复边界、风格样本、任务判断、安全限制和用户反馈迭代规则的组合。</p>"
        "<ul><li>原 Developer/User Prompt</li><li>Prompt 变量和动态函数</li>"
        "<li>Work Profile 生成的判断顺序</li><li>人工纠偏样本形成的规则更新入口</li></ul>"
        "</article>"
        "<article class=\"domain-card\">"
        "<h3>Memory 是什么</h3>"
        "<p>Memory 负责回答“这件事的长期背景是什么”。原项目已经使用 memory_connector，"
        "同时还有本地工作画像、风格语料、组织信息缓存和项目 memory_context。</p>"
        "<ul><li>Memory MCP 的召回能力</li><li>钉钉材料蒸馏出的工作画像</li>"
        "<li>历史回复与人工反馈</li><li>联系人、部门、项目上下文</li></ul>"
        "</article>"
        "<article class=\"domain-card domain-card-wide\">"
        "<h3>这版不做的事</h3>"
        "<p class=\"muted\">这里不暴露安装向导和研发调试入口；"
        "Recipe / Memory 的展示以当前配置、文件和运行数据为来源。</p>"
        "</article>"
        "</div>"
        "</section>"
    )


def _render_recipe_package(*, saved: bool = False) -> str:
    saved_html = "<p class=\"config-note\">已保存。</p>" if saved else ""
    return (
        "<section class=\"card\">"
        "<h2>Recipe 包</h2>"
        "<p class=\"muted\">Developer Prompt 是这套 Recipe 的主规则。变量、动态上下文和 User Prompt "
        "共同完成渲染，但系统的身份、判断边界、回复原则和禁止事项优先在主规则里维护。</p>"
        f"{saved_html}"
        "<div class=\"domain-grid\">"
        "<article class=\"domain-card\">"
        "<h3>当前身份</h3>"
        f"{_config_kv([('代理对象', user_alias()), ('分身签名', assistant_signature()), ('转人工提示', handoff_ack())])}"
        "</article>"
        "<article class=\"domain-card\">"
        "<h3>触发边界</h3>"
        f"{_config_kv([('禁止输出', _csv_label(forbidden_path_prefixes())), ('可识别称呼', _csv_label(mention_aliases())), ('广播称呼', _csv_label(broadcast_mention_aliases()))])}"
        "</article>"
        "<article class=\"domain-card domain-card-wide\">"
        "<h3>Recipe 变量</h3>"
        "<p class=\"muted\">这些变量会进入 Prompt 渲染，适合维护经常调整但不需要改主规则结构的内容。</p>"
        f"{_config_variable_form('recipe')}"
        "</article>"
        "</div>"
        "</section>"
        "<section class=\"card\">"
        "<h2>主规则（Developer Prompt）</h2>"
        "<p class=\"muted\">这里是 Recipe 包的核心编辑区。修改后会影响消息判断、回复边界、风格要求和安全限制。</p>"
        "</section>"
        f"{_render_developer_prompt_editor_content(saved=False)}"
        "<section class=\"card\">"
        "<h2>辅助渲染</h2>"
        "<details class=\"config-collapse config-collapse-spaced\">"
        "<summary><h3>动态函数</h3></summary>"
        f"{_user_prompt_dynamic_function_table()}"
        "</details>"
        "<details class=\"config-collapse config-collapse-spaced\">"
        "<summary><h3>User Prompt</h3></summary>"
        f"{_render_user_prompt_editor_content(saved=False)}"
        "</details>"
        "</section>"
    )


def _render_memory_view(*, db_path: Path | None = None) -> str:
    store = AutoReplyStore(db_path) if db_path is not None else None
    profile_path = work_profile_path()
    evidence_path = Path("data/profile-evidence/evidence_index.jsonl")
    style_path = corpus_dir() / "style_corpus.csv"
    org_count = _count_table_rows(store, "org_user_profiles") if store else 0
    project_memory_count = _count_non_empty_project_memory_context(store) if store else 0
    reviewed_count = len(store.list_reviewed_reply_attempts(limit=500)) if store else 0
    return (
        f"{_render_memory_connection_panel(profile_path=profile_path, evidence_path=evidence_path, style_path=style_path)}"
        "<section class=\"card\">"
        "<h2>记忆资产</h2>"
        "<p class=\"muted\">这些数据会在回复、任务判断和项目上下文补全时被使用。</p>"
        "<div class=\"domain-grid\">"
        "<article class=\"domain-card\">"
        "<h3>工作画像</h3>"
        f"{_config_kv([('Profile 文件', _file_state(profile_path)), ('证据索引', _file_state(evidence_path)), ('回复风格语料', _file_state(style_path))])}"
        "</article>"
        "<article class=\"domain-card\">"
        "<h3>运行时记忆</h3>"
        f"{_config_kv([('组织联系人缓存', f'{org_count} 人'), ('项目 memory_context', f'{project_memory_count} 个'), ('人工纠偏样本', f'{reviewed_count} 条')])}"
        "</article>"
        "<article class=\"domain-card\">"
        "<h3>本地材料</h3>"
        f"{_config_kv([('Workspace', str(workspace_path())), ('Corpus', str(corpus_dir())), ('Worker DB', str(worker_db_path()))])}"
        "</article>"
        "</div>"
        "</section>"
    )


def _render_memory_connection_panel(
    *,
    profile_path: Path,
    evidence_path: Path,
    style_path: Path,
) -> str:
    connector_ready = _codex_memory_connector_configured()
    local_ready = profile_path.exists() or evidence_path.exists() or style_path.exists()
    ready = connector_ready or local_ready
    status_text = "可用" if ready else "需要配置"
    status_detail = (
        "Memory Connector 已配置，可用于召回长期背景。"
        if connector_ready
        else "本地画像和语料可用；Memory Connector 未检测到。"
        if local_ready
        else "连接或导入材料后，工作台才能使用长期记忆。"
    )
    status_class = "" if ready else " needs-action"
    memory_url = _memory_console_url()
    meta_items = [
        f"<span class=\"pill status-sent\">{escape(status_text)}</span>"
        if ready
        else f"<span class=\"pill status-failed\">{escape(status_text)}</span>",
        f"<span class=\"pill\">{escape('Connector 已配置' if connector_ready else 'Connector 未检测到')}</span>",
    ]
    if local_ready:
        meta_items.append("<span class=\"pill\">本地材料可用</span>")
    return (
        "<section class=\"card memory-connection-card\">"
        "<div class=\"dingtalk-connection-head\">"
        "<div class=\"dingtalk-account\">"
        f"<span class=\"memory-status-dot{status_class}\" aria-hidden=\"true\"></span>"
        "<div>"
        "<div class=\"dingtalk-eyebrow\">Memory 状态</div>"
        f"<h2>{escape(status_text)}</h2>"
        f"<div class=\"dingtalk-account-id\">空间：{escape(memory_connector_user_id() or '使用当前 Friday 账号')}</div>"
        f"<p class=\"muted\">{escape(status_detail)}</p>"
        f"<div class=\"dingtalk-meta\">{''.join(meta_items)}</div>"
        "</div>"
        "</div>"
        "<div class=\"dingtalk-action-row\">"
        f"<a class=\"secondary-button\" href=\"{escape(memory_url)}\" target=\"_blank\" rel=\"noopener\">打开 Memory</a>"
        "<a class=\"secondary-button\" href=\"/config?tab=memory\">刷新状态</a>"
        "</div>"
        "</div>"
        "<div class=\"memory-source-grid\">"
        f"{_memory_source_card('长期背景召回', '已接入' if connector_ready else '未接入', '用于查找跨项目、跨会话的长期事实。')}"
        f"{_memory_source_card('工作画像', '已生成' if profile_path.exists() else '未生成', '用于稳定回复风格、判断顺序和责任边界。')}"
        f"{_memory_source_card('反馈沉淀', '可用', '用户反馈和历史处理记录会作为后续判断依据。')}"
        "</div>"
        "</section>"
    )


def _memory_source_card(label: str, value: str, description: str) -> str:
    return (
        "<div class=\"memory-source-card\">"
        f"<div class=\"memory-source-label\">{escape(label)}</div>"
        f"<div class=\"memory-source-value\">{escape(value)}</div>"
        f"<div class=\"memory-source-desc\">{escape(description)}</div>"
        "</div>"
    )


def _memory_console_url() -> str:
    return os.getenv("FRIDAY_MEMORY_CONSOLE_URL", "https://friday.stardust.ai").strip() or "https://friday.stardust.ai"


def _render_runtime_config_view(
    *,
    saved: bool = False,
    dingtalk_reconnect: str = "",
    db_path: Path | None = None,
) -> str:
    saved_html = "<p class=\"config-note\">已保存。</p>" if saved else ""
    store = AutoReplyStore(db_path) if db_path is not None else None
    reconnect_html = ""
    if dingtalk_reconnect == "reconnect-started":
        reconnect_html = "<p class=\"config-note\">已开始重新连接钉钉。请按弹出的登录流程完成授权，然后刷新状态。</p>"
    elif dingtalk_reconnect == "reconnect-failed":
        reconnect_html = "<p class=\"config-note config-warning-note\">重新连接没有启动成功。请检查 dws auth login 是否可用。</p>"
    return (
        f"{_render_dingtalk_connection_panel(store)}"
        "<section class=\"card\">"
        "<h2>钉钉与自动回复</h2>"
        "<p class=\"muted\">管理消息进入、同步范围、自动回复策略和运行节奏。</p>"
        f"{saved_html}{reconnect_html}"
        "<div class=\"domain-grid\">"
        "<article class=\"domain-card\">"
        "<h3>消息范围</h3>"
        f"{_config_kv([('点名别名', _csv_label(mention_aliases())), ('广播别名', _csv_label(broadcast_mention_aliases())), ('材料抽取身份', _csv_label(document_extraction_ids())), ('私聊恢复窗口', _duration_label(single_chat_read_recovery_window()))])}"
        "</article>"
        "<article class=\"domain-card\">"
        "<h3>回复模式</h3>"
        f"{_config_kv([('当前模式', _current_reply_mode_label()), ('仅回复私聊', _enabled_label(single_chat_only())), ('反馈入口', feedback_spike_vercel_base_url() or '未配置'), ('回复签名', assistant_signature())])}"
        "</article>"
        "</div>"
        "</section>"
        "<form method=\"post\" action=\"/config/system\">"
        f"{_runtime_config_group('钉钉同步', '控制钉钉账号、消息范围、材料抽取和只回复私聊策略。', ['CEO_PRINCIPAL_NAME', 'USER_ALIAS', 'CEO_SINGLE_CHAT_ONLY', 'CEO_MENTION_ALIASES', 'CEO_BROADCAST_MENTION_ALIASES', 'DOCUMENT_EXTRACTION_IDS'])}"
        f"{_runtime_config_group('自动回复', '控制系统是否真实发送、是否只生成记录，以及回复签名和转人工文案。', ['CEO_NOT_SEND_MESSAGE', 'CEO_DRY_RUN', 'CEO_ASSISTANT_SIGNATURE', 'CEO_HANDOFF_ACK', 'CEO_FEEDBACK_SPIKE_VERCEL_BASE_URL', 'CEO_FORBIDDEN_PATH_PREFIXES'])}"
        f"{_runtime_config_group('同步节奏', '控制未读消息、任务消费和已读私聊恢复的运行频率。', ['CEO_PRODUCER_INTERVAL_SECONDS', 'CEO_CONSUMER_POLL_INTERVAL_SECONDS', 'CEO_POLL_INTERVAL_SECONDS', 'CEO_BATCH_SECONDS', 'FAST_PATH_UNREAD_BACKOFF', 'MESSAGE_RECOVERY_INTERVAL', 'SINGLE_CHAT_READ_RECOVERY_WINDOW', 'SINGLE_CHAT_READ_RECOVERY_LIMIT'])}"
        f"{_runtime_config_group('本地材料', '控制工作画像、语料和运行数据库的位置。', ['CEO_WORKSPACE', 'CEO_WORKER_DB', 'CEO_CORPUS_DIR', 'CEO_WORK_PROFILE_PATH'])}"
        "<p><button type=\"submit\">保存钉钉与回复配置</button></p>"
        "</form>"
        f"{_runtime_identity_cache_html(db_path)}"
    )


def _render_dingtalk_connection_panel(store: AutoReplyStore | None) -> str:
    current_user_id = store.get_current_user_id() if store is not None else ""
    profile = store.get_org_user_profile(current_user_id) if store and current_user_id else None
    connected = bool(current_user_id)
    display_name = (
        profile.name
        if profile and profile.name
        else user_alias()
        if connected
        else "未连接钉钉"
    )
    title = profile.title if profile and profile.title else ""
    department = (
        " / ".join(sorted(profile.department_names))
        if profile and profile.department_names
        else ""
    )
    org_label = "、".join(profile.org_labels) if profile and profile.org_labels else ""
    status_text = "已连接" if connected else "需要连接"
    detail_text = (
        "已缓存当前账号，可用于识别本人消息。"
        if connected and profile
        else "已缓存当前账号，姓名和组织详情待下次同步补齐。"
        if connected
        else "连接后才能读取钉钉私聊、上下文和组织信息。"
    )
    status_class = "" if connected else " needs-action"
    meta_items = [
        f"<span id=\"dingtalk-status-pill\" class=\"pill status-sent\">{escape(status_text)}</span>"
        if connected
        else f"<span id=\"dingtalk-status-pill\" class=\"pill status-failed\">{escape(status_text)}</span>",
    ]
    if title:
        meta_items.append(f"<span class=\"pill\">{escape(title)}</span>")
    if department:
        meta_items.append(f"<span class=\"pill\">{escape(department)}</span>")
    if org_label:
        meta_items.append(f"<span class=\"pill\">{escape(org_label)}</span>")
    return (
        "<section class=\"card dingtalk-connection-card\">"
        "<div class=\"dingtalk-connection-head\">"
        "<div class=\"dingtalk-account\">"
        f"<span id=\"dingtalk-status-dot\" class=\"dingtalk-status-dot{status_class}\" aria-hidden=\"true\"></span>"
        "<div>"
        "<div class=\"dingtalk-eyebrow\">当前钉钉账号</div>"
        f"<h2 id=\"dingtalk-display-name\">{escape(display_name)}</h2>"
        f"<div id=\"dingtalk-account-id\" class=\"dingtalk-account-id\">userId: {escape(current_user_id or '未缓存')}</div>"
        f"<p id=\"dingtalk-detail-text\" class=\"muted\">{escape(detail_text)}</p>"
        f"<div class=\"dingtalk-meta\">{''.join(meta_items)}</div>"
        "</div>"
        "</div>"
        "<div class=\"dingtalk-action-row\">"
        "<form method=\"post\" action=\"/config/dingtalk/reconnect\">"
        "<button type=\"submit\">重新连接钉钉</button>"
        "</form>"
        "<a class=\"secondary-button\" href=\"/config?tab=runtime\">刷新状态</a>"
        "</div>"
        "</div>"
        "<div class=\"dingtalk-checks\">"
        f"{_dingtalk_connection_check('账号身份', '已确认' if connected else '未确认', '用于过滤本人消息，避免系统回复自己。', value_id='dingtalk-identity-status')}"
        f"{_dingtalk_connection_check('同步范围', '仅私聊' if single_chat_only() else '私聊 + 群聊候选', '当前工作台默认优先处理私聊消息。')}"
        f"{_dingtalk_connection_check('发送状态', _current_reply_mode_label(), '由 Dry Run 和禁止发送开关共同决定。')}"
        f"{_dingtalk_connection_check('Token 状态', '正在校验', '由 dws auth status 实时检查登录有效期。', value_id='dingtalk-token-status')}"
        f"{_dingtalk_connection_check('环境检查', '正在校验', '由 dws doctor 检查网络、缓存和版本状态。', value_id='dingtalk-doctor-status')}"
        "</div>"
        f"{_dingtalk_connection_status_script()}"
        "</section>"
    )


def _dingtalk_connection_check(
    label: str,
    value: str,
    description: str,
    *,
    value_id: str = "",
) -> str:
    id_attr = f" id=\"{escape(value_id)}\"" if value_id else ""
    return (
        "<div class=\"dingtalk-check\">"
        f"<div class=\"dingtalk-check-label\">{escape(label)}</div>"
        f"<div{id_attr} class=\"dingtalk-check-value\">{escape(value)}</div>"
        f"<div class=\"dingtalk-check-desc\">{escape(description)}</div>"
        "</div>"
    )


def _dingtalk_connection_status_script() -> str:
    return """
<script data-dingtalk-connection-status>
(() => {
  const statusDot = document.getElementById("dingtalk-status-dot");
  const statusPill = document.getElementById("dingtalk-status-pill");
  const displayName = document.getElementById("dingtalk-display-name");
  const accountId = document.getElementById("dingtalk-account-id");
  const detailText = document.getElementById("dingtalk-detail-text");
  const identityStatus = document.getElementById("dingtalk-identity-status");
  const tokenStatus = document.getElementById("dingtalk-token-status");
  const doctorStatus = document.getElementById("dingtalk-doctor-status");
  const params = new URLSearchParams(window.location.search);
  let remainingPolls = params.get("dingtalk") === "reconnect-started" ? 30 : 1;

  const setStatusPill = (connected, label) => {
    if (!statusPill) return;
    statusPill.textContent = label;
    statusPill.classList.toggle("status-sent", connected);
    statusPill.classList.toggle("status-failed", !connected);
    if (statusDot) {
      statusDot.classList.toggle("needs-action", !connected);
    }
  };
  const shortTime = (value) => {
    if (!value) return "";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return value;
    return date.toLocaleString();
  };
  const render = (data) => {
    const connected = Boolean(data.connected);
    setStatusPill(connected, connected ? "已连接" : "需要连接");
    if (displayName) {
      displayName.textContent = data.display_name || (connected ? "钉钉账号已连接" : "未连接钉钉");
    }
    if (accountId) {
      accountId.textContent = `userId: ${data.user_id || "未缓存"}`;
    }
    if (detailText) {
      detailText.textContent = data.detail || (connected ? "已通过 dws 实时确认登录状态。" : "连接后才能读取钉钉私聊、上下文和组织信息。");
    }
    if (identityStatus) {
      identityStatus.textContent = connected ? "已确认" : "未确认";
    }
    if (tokenStatus) {
      tokenStatus.textContent = connected
        ? `有效至 ${shortTime(data.expires_at) || "未知时间"}`
        : (data.auth_error || "未登录");
    }
    if (doctorStatus) {
      doctorStatus.textContent = data.doctor_label || "未完成";
    }
  };
  const poll = async () => {
    try {
      const response = await fetch("/api/dingtalk/status", {
        cache: "no-store",
        headers: {"Accept": "application/json"},
      });
      if (response.ok) {
        render(await response.json());
      }
    } finally {
      remainingPolls -= 1;
      if (remainingPolls > 0) {
        setTimeout(poll, 2000);
      }
    }
  };
  poll();
})();
</script>
"""


def dingtalk_connection_status(
    store: AutoReplyStore,
    dws: DwsClient | None = None,
) -> dict[str, object]:
    client = dws or DwsClient(timeout_seconds=8)
    status: dict[str, object] = {
        "connected": False,
        "authenticated": False,
        "token_valid": False,
        "refresh_token_valid": False,
        "user_id": store.get_current_user_id() or "",
        "display_name": "",
        "expires_at": "",
        "refresh_expires_at": "",
        "corp_id": "",
        "doctor_label": "未完成",
        "detail": "正在检查钉钉连接状态。",
        "auth_error": "",
        "doctor_error": "",
    }

    try:
        auth = client.auth_status()
    except Exception as exc:
        status["auth_error"] = _public_error_message(exc)
        status["detail"] = "无法读取 dws 登录状态，请重新连接钉钉。"
        return status

    authenticated = bool(auth.get("authenticated"))
    token_valid = bool(auth.get("token_valid"))
    status.update(
        {
            "authenticated": authenticated,
            "token_valid": token_valid,
            "refresh_token_valid": bool(auth.get("refresh_token_valid")),
            "expires_at": str(auth.get("expires_at") or ""),
            "refresh_expires_at": str(auth.get("refresh_expires_at") or ""),
            "corp_id": str(auth.get("corp_id") or ""),
        }
    )
    if not authenticated or not token_valid:
        status["auth_error"] = "未登录或 token 已失效"
        status["detail"] = "请重新连接钉钉，完成浏览器授权后系统会自动刷新状态。"
        return status

    profile = None
    try:
        profile = client.get_current_user_profile()
        _cache_current_dingtalk_profile(store, profile)
        status["user_id"] = profile.user_id
        status["display_name"] = profile.name or profile.user_id
    except Exception as exc:
        status["auth_error"] = _public_error_message(exc)
        status["detail"] = "dws 已登录，但当前账号信息读取失败。"

    try:
        doctor = client.doctor(timeout_seconds=5)
        status["doctor_label"] = _dws_doctor_label(doctor)
    except Exception as exc:
        status["doctor_error"] = _public_error_message(exc)
        status["doctor_label"] = "环境检查失败"

    status["connected"] = bool(status["user_id"]) and authenticated and token_valid
    if status["connected"]:
        parts = ["已通过 dws 实时确认登录状态"]
        if profile and profile.department_names:
            parts.append("部门：" + " / ".join(sorted(profile.department_names)))
        if status["doctor_label"]:
            parts.append("环境：" + str(status["doctor_label"]))
        status["detail"] = "；".join(parts) + "。"
    return status


def _cache_current_dingtalk_profile(
    store: AutoReplyStore,
    profile: DwsUserProfile,
) -> None:
    store.set_current_user_id(profile.user_id)
    store.upsert_org_user_profile(
        user_id=profile.user_id,
        name=profile.name,
        title=profile.title,
        open_dingtalk_id=profile.open_dingtalk_id,
        manager_user_id=profile.manager_user_id,
        manager_name=profile.manager_name,
        department_ids=profile.department_ids,
        department_names=profile.department_names,
        org_labels=profile.org_labels,
        has_subordinate=profile.has_subordinate,
    )


def _dws_doctor_label(payload: dict[str, object]) -> str:
    summary = payload.get("summary")
    if isinstance(summary, dict):
        fail = int(summary.get("fail") or 0)
        warn = int(summary.get("warn") or 0)
        passed = int(summary.get("pass") or 0)
        if fail:
            return f"{fail} 项失败"
        if warn:
            return f"{passed} 项通过，{warn} 项提醒"
        return f"{passed} 项通过"
    checks = payload.get("checks")
    if isinstance(checks, list):
        failures = sum(
            1
            for check in checks
            if isinstance(check, dict) and str(check.get("status")) == "fail"
        )
        if failures:
            return f"{failures} 项失败"
        return f"{len(checks)} 项通过"
    return "已完成"


def _public_error_message(exc: object) -> str:
    text = str(exc).strip()
    if not text:
        return "未知错误"
    return text[:240]


def _current_reply_mode_label() -> str:
    not_send = _env_flag("CEO_NOT_SEND_MESSAGE", False)
    dry_run = _env_flag("CEO_DRY_RUN", False)
    if dry_run or not_send:
        return "仅生成不发送"
    return "自动发送"


def _runtime_config_group(title: str, description: str, keys: list[str]) -> str:
    editable_keys = _editable_system_config_keys()
    row_map = {key: (value, detail) for key, value, detail in _system_config_rows()}
    rows: list[str] = ["<tr><th>配置项</th><th>当前值</th><th>说明</th></tr>"]
    for key in keys:
        value, detail = row_map.get(key, ("", "来自 .env；服务启动或 prompt/config 渲染时读取。"))
        editable = key in editable_keys
        rows.append(
            "<tr>"
            f"<td>{_system_config_key_cell(key, editable)}</td>"
            f"<td>{_system_config_value_cell(key, value, editable)}</td>"
            f"<td>{escape(detail)}</td>"
            "</tr>"
        )
    return (
        "<section class=\"card\">"
        f"<h2>{escape(title)}</h2>"
        f"<p class=\"muted\">{escape(description)}</p>"
        "<table class=\"system-config-table\">"
        + "".join(rows)
        + "</table>"
        "</section>"
    )


def _render_dingtalk_sync_view(*, db_path: Path | None = None) -> str:
    store = AutoReplyStore(db_path) if db_path is not None else None
    current_user_id = store.get_current_user_id() if store is not None else ""
    return (
        "<section class=\"card\">"
        "<h2>钉钉同步范围</h2>"
        "<p class=\"muted\">这部分对应原项目的数据入口：连接钉钉后，系统从消息、文档、会议纪要、"
        "组织信息和已读恢复里获得上下文，再进入 Recipe / Memory 判断。</p>"
        "<div class=\"domain-grid\">"
        "<article class=\"domain-card\">"
        "<h3>账号与会话</h3>"
        f"{_config_kv([('当前本人 ID', current_user_id or '未缓存'), ('仅回复私聊', _enabled_label(single_chat_only())), ('私聊恢复窗口', _duration_label(single_chat_read_recovery_window())), ('恢复扫描上限', str(single_chat_read_recovery_limit()))])}"
        "</article>"
        "<article class=\"domain-card\">"
        "<h3>触发范围</h3>"
        f"{_config_kv([('点名别名', _csv_label(mention_aliases())), ('广播别名', _csv_label(broadcast_mention_aliases())), ('材料抽取身份', _csv_label(document_extraction_ids()))])}"
        "</article>"
        "<article class=\"domain-card\">"
        "<h3>材料目录</h3>"
        f"{_config_kv([('Workspace', str(workspace_path())), ('Corpus', str(corpus_dir())), ('Work Profile', str(work_profile_path()))])}"
        "</article>"
        "<article class=\"domain-card\">"
        "<h3>同步节奏</h3>"
        f"{_config_kv([('Producer 间隔', f'{producer_interval_seconds()} 秒'), ('Consumer 间隔', f'{consumer_poll_interval_seconds()} 秒'), ('未读回看等待', _duration_label(fast_path_unread_backoff_duration())), ('慢路径恢复间隔', _duration_label(message_recovery_interval()))])}"
        "</article>"
        "</div>"
        "</section>"
    )


def _render_auto_reply_strategy() -> str:
    not_send = _env_flag("CEO_NOT_SEND_MESSAGE", False)
    dry_run = _env_flag("CEO_DRY_RUN", False)
    return (
        "<section class=\"card\">"
        "<h2>自动回复策略</h2>"
        "<p class=\"muted\">这里对应原项目真正发消息前后的策略：是否自动发送、是否只记录、"
        "如何签名、如何转人工，以及如何收集外部反馈。</p>"
        "<div class=\"domain-grid\">"
        "<article class=\"domain-card\">"
        "<h3>发送模式</h3>"
        f"{_config_kv([('Dry Run', _enabled_label(dry_run)), ('禁止发送', _enabled_label(not_send)), ('当前模式', '仅生成不发送' if dry_run or not_send else '自动发送'), ('回复签名', assistant_signature())])}"
        "</article>"
        "<article class=\"domain-card\">"
        "<h3>低信心处理</h3>"
        f"{_config_kv([('转人工文案', handoff_ack()), ('反馈入口', feedback_spike_vercel_base_url() or '未配置'), ('安全路径拦截', _csv_label(forbidden_path_prefixes()))])}"
        "</article>"
        "<article class=\"domain-card domain-card-wide\">"
        "<h3>迭代关系</h3>"
        "<p class=\"muted\">用户反馈不会直接变成 Memory；它先作为纠偏样本进入处理记录，"
        "同类消息生成回复时会被召回影响判断边界。Friday 产品化后，可以把稳定反馈进一步合并进 Recipe。</p>"
        "</article>"
        "</div>"
        "</section>"
    )


def _render_full_config(*, saved: bool = False, db_path: Path | None = None) -> str:
    saved_html = "<p class=\"config-note\">已保存。</p>" if saved else ""
    return (
        "<section class=\"card\">"
        "<h2>高级参数</h2>"
        "<p class=\"muted\">这是研发诊断入口，不作为投资人演示主路径。"
        "Recipe、Memory、钉钉同步和自动回复策略已经在前面页面产品化展示；"
        "这里仅保留服务运行时仍可能需要核对的环境参数。</p>"
        f"{saved_html}"
        "</section>"
        f"{_render_system_config(db_path=db_path)}"
    )


def _prompt_config_card(active_tab: str) -> str:
    return (
        "<section class=\"card\">"
        "<h2>Prompt 配置</h2>"
        "<p class=\"muted\">Developer Prompt 和 User Prompt 渲染时共用的配置。</p>"
        "<details class=\"config-collapse\">"
        "<summary><h3>配置变量</h3></summary>"
        f"{_config_variable_form(active_tab)}"
        "</details>"
        "<details class=\"config-collapse\">"
        "<summary><h3>动态函数</h3></summary>"
        f"{_user_prompt_dynamic_function_table()}"
        "</details>"
        "</section>"
    )


def _config_tabs(active_tab: str) -> str:
    tabs = [
        ("recipe", "Recipe 包"),
        ("memory", "Memory"),
        ("runtime", "钉钉与自动回复"),
    ]
    return (
        "<nav class=\"prompt-tabs\" aria-label=\"配置分组\">"
        + "".join(
            f"<a class=\"{'prompt-tab active' if key == active_tab else 'prompt-tab'}\" "
            f"href=\"/config?tab={escape(key)}\">{escape(label)}</a>"
            for key, label in tabs
        )
        + "</nav>"
    )


def _config_stat(label: str, value: str) -> str:
    return (
        "<div class=\"config-stat\">"
        f"<div class=\"config-stat-label\">{escape(label)}</div>"
        f"<div class=\"config-stat-value\">{escape(value)}</div>"
        "</div>"
    )


def _config_stage(index: str, title: str, description: str) -> str:
    return (
        "<div class=\"config-stage\">"
        f"<div class=\"config-stage-num\">{escape(index)}</div>"
        f"<div class=\"config-stage-title\">{escape(title)}</div>"
        f"<div class=\"config-stage-desc\">{escape(description)}</div>"
        "</div>"
    )


def _config_kv(rows: list[tuple[str, str]]) -> str:
    return (
        "<div class=\"config-kv\">"
        + "".join(
            "<div class=\"config-kv-row\">"
            f"<div class=\"config-kv-label\">{escape(label)}</div>"
            f"<div class=\"config-kv-value\">{escape(value)}</div>"
            "</div>"
            for label, value in rows
        )
        + "</div>"
    )


def _enabled_label(value: bool) -> str:
    return "开启" if value else "关闭"


def _status_text(value: bool, enabled: str, disabled: str) -> str:
    return enabled if value else disabled


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _codex_memory_connector_configured() -> bool:
    try:
        return codex_config_has_memory_connector(Path.home() / ".codex" / "config.toml")
    except OSError:
        return False


def _file_state(path: Path) -> str:
    if not path.exists():
        return f"缺失：{path}"
    size = path.stat().st_size
    line_count = _count_file_lines(path)
    return f"已生成：{path}（{_byte_size_label(size)}，{line_count} 行）"


def _count_file_lines(path: Path) -> int:
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            return sum(1 for _ in handle)
    except OSError:
        return 0


def _byte_size_label(size: int) -> str:
    if size >= 1024 * 1024:
        return f"{size / (1024 * 1024):.1f} MB"
    if size >= 1024:
        return f"{size / 1024:.1f} KB"
    return f"{size} B"


def _count_table_rows(store: AutoReplyStore, table: str) -> int:
    if table not in {"org_user_profiles"}:
        return 0
    try:
        with store._connect() as db:
            row = db.execute(f"select count(*) as count from {table}").fetchone()
            return int(row["count"])
    except Exception:
        return 0


def _count_non_empty_project_memory_context(store: AutoReplyStore) -> int:
    try:
        with store._connect() as db:
            row = db.execute(
                """
                select count(*) as count
                from work_projects
                where trim(coalesce(memory_context_json, '')) not in ('', '{}')
                """
            ).fetchone()
            return int(row["count"])
    except Exception:
        return 0


def _config_variable_form(active_tab: str) -> str:
    try:
        variable_inputs = _config_variable_inputs()
        error_html = ""
    except (OSError, DeveloperPromptTemplateError) as exc:
        variable_inputs = ""
        error_html = (
            "<p class=\"attempt-warning\">"
            f"无法加载变量：{escape(str(exc))}"
            "</p>"
        )
    return (
        f"{error_html}"
        "<form method=\"post\" action=\"/config/variables\">"
        f"<input type=\"hidden\" name=\"active_tab\" value=\"{escape(active_tab)}\">"
        f"{variable_inputs}"
        "<p><button type=\"submit\">保存变量</button></p>"
        "</form>"
    )


def _render_config_info() -> str:
    logic_sections = _config_logic_sections()
    logic_html = "".join(
        "<section class=\"logic-section\">"
        f"<h3>{escape(title)}</h3>"
        "<dl>"
        + "".join(
            f"<div><dt>{escape(label)}</dt><dd>{_highlight_logic_text(description)}</dd></div>"
            for label, description in rows
        )
        + "</dl>"
        "</section>"
        for title, rows in logic_sections
    )
    return (
        "<section class=\"card\">"
        "<h2>Producer 路由配置</h2>"
        "<p class=\"muted\">这里展示 producer 如何把钉钉消息变成 reply task。</p>"
        f"<div class=\"logic-list\">{logic_html}</div>"
        "</section>"
    )


def _system_config_rows() -> list[tuple[str, str, str]]:
    env_values = read_env_file()
    mention_text = _csv_label(mention_aliases())
    broadcast_text = _csv_label(broadcast_mention_aliases())
    document_extraction_text = _csv_label(document_extraction_ids())
    forbidden_path_text = _csv_label(forbidden_path_prefixes())
    known_rows = [
        (
            "CEO_PRINCIPAL_NAME",
            principal_name(),
            "代理对象账号名称；用于系统内部识别 principal。",
        ),
        (
            "USER_ALIAS",
            user_alias(),
            "用户别名；用于展示、handoff 文案、日历/profile 等运行时文案。",
        ),
        (
            "MEMORY_CONNECTOR_USER_ID",
            memory_connector_user_id(),
            "Memory Connector 的用户空间；用于 MCP header 和 prompt 中的 memory user_id。",
        ),
        (
            "CEO_MENTION_ALIASES",
            mention_text,
            "群聊/消息触发时识别点名 principal 的别名；影响 producer 候选生成。",
        ),
        (
            "CEO_BROADCAST_MENTION_ALIASES",
            broadcast_text,
            "识别 @所有人、@all 等广播消息；群聊广播也会进入候选判断。",
        ),
        (
            "CEO_SINGLE_CHAT_ONLY",
            "1" if single_chat_only() else "0",
            "限制候选消息来源；开启后仅处理一对一私聊，群消息不自动回复。",
        ),
        (
            "DOCUMENT_EXTRACTION_IDS",
            document_extraction_text,
            "用于从会议纪要和文档语料中抽取该身份的发言或材料。",
        ),
        (
            "CEO_ASSISTANT_SIGNATURE",
            assistant_signature(),
            "服务发送回复时追加的分身签名。",
        ),
        (
            "CEO_HANDOFF_ACK",
            handoff_ack(),
            "系统需要交给真人处理时的默认提示文案。",
        ),
        (
            "CEO_FEEDBACK_SPIKE_VERCEL_BASE_URL",
            feedback_spike_vercel_base_url(),
            "对话方反馈页根地址；配置后发出的回复会自动追加赞踩链接并记录 feedback token。",
        ),
        (
            "CEO_NOT_SEND_MESSAGE",
            "1" if _env_flag("CEO_NOT_SEND_MESSAGE", False) else "0",
            "禁止服务把生成结果发送到钉钉；开启后只保留处理记录。",
        ),
        (
            "CEO_DRY_RUN",
            "1" if _env_flag("CEO_DRY_RUN", False) else "0",
            "预演模式；开启后仍生成回复和审计记录，但不会真实发送钉钉消息。",
        ),
        (
            "CEO_WORKSPACE",
            str(workspace_path()),
            "本地知识库路径；自动处理服务和 graphify 从这里读取业务材料。",
        ),
        (
            "CEO_WORKER_DB",
            str(worker_db_path()),
            "本地 SQLite 运行状态和审计数据库路径。",
        ),
        (
            "CEO_CORPUS_DIR",
            str(corpus_dir()),
            "回复风格语料和检索语料的本地目录。",
        ),
        (
            "CEO_WORK_PROFILE_PATH",
            str(work_profile_path()),
            "work_profile_instruction() 读取这个文件并注入 Developer Prompt。",
        ),
        (
            "CEO_FORBIDDEN_PATH_PREFIXES",
            forbidden_path_text,
            "系统安全检查使用：按路径前缀识别本机路径泄漏。",
        ),
        (
            "CEO_PRODUCER_INTERVAL_SECONDS",
            str(producer_interval_seconds()),
            "主服务内 producer loop 的运行间隔。",
        ),
        (
            "CEO_CONSUMER_POLL_INTERVAL_SECONDS",
            str(consumer_poll_interval_seconds()),
            "consumer 检查 pending reply task 的间隔秒数。",
        ),
        (
            "CEO_POLL_INTERVAL_SECONDS",
            str(poll_interval_seconds()),
            "本地 run 模式下，快路径轮询未读会话的间隔秒数。",
        ),
        (
            "CEO_BATCH_SECONDS",
            str(batch_seconds()),
            "本地 run 模式下，每个消息发现批次覆盖的时间窗口秒数。",
        ),
        (
            "FAST_PATH_UNREAD_BACKOFF",
            _duration_label(fast_path_unread_backoff_duration()),
            "快路径扫描到未读会话后等待多久再读取，给真人先回复或清未读的时间。",
        ),
        (
            "MESSAGE_RECOVERY_INTERVAL",
            _duration_label(message_recovery_interval()),
            "每次慢路径兜底扫描之间至少间隔多久。",
        ),
        (
            "SINGLE_CHAT_READ_RECOVERY_WINDOW",
            _duration_label(single_chat_read_recovery_window()),
            "慢路径私聊恢复扫描回看多长时间内的会话。",
        ),
        (
            "SINGLE_CHAT_READ_RECOVERY_LIMIT",
            str(single_chat_read_recovery_limit()),
            "慢路径私聊恢复扫描最多读取多少个会话。",
        ),
    ]
    descriptions = {key: description for key, _, description in known_rows}
    values = {key: value for key, value, _ in known_rows}
    ordered_keys = [key for key, _, _ in known_rows]
    for key in env_values:
        if key not in values:
            ordered_keys.append(key)
    return [
        (
            key,
            env_values.get(key, values.get(key, "")),
            descriptions.get(key, "来自 .env；服务启动或 prompt/config 渲染时读取。"),
        )
        for key in ordered_keys
    ]


def _config_variable_inputs() -> str:
    rows: list[str] = ["<tr><th>Key</th><th>Value</th></tr>"]
    for key, value in configurable_prompt_variable_pairs():
        rows.append(_variable_input_row(key, value))
    return "<table class=\"config-variable-table\">" + "".join(rows) + "</table>"


def _variable_input_row(key: str, value: str) -> str:
    env_key = prompt_variable_env_key(key)
    return (
        "<tr>"
        f"<td><code class=\"config-value\">{escape(env_key)}</code>"
        f"<input type=\"hidden\" name=\"variable_key\" value=\"{escape(env_key)}\"></td>"
        f"<td><input class=\"config-value-input\" type=\"text\" name=\"variable_value\" value=\"{escape(value)}\"></td>"
        "</tr>"
    )


def _developer_prompt_variable_map() -> dict[str, str]:
    return dict(configurable_prompt_variable_pairs())


def _render_system_config(*, db_path: Path | None = None) -> str:
    editable_keys = _editable_system_config_keys()
    rows = [
        "<tr><th>配置项</th><th>当前值</th><th>用途</th></tr>",
        *[
            "<tr>"
            f"<td>{_system_config_key_cell(key, key in editable_keys)}</td>"
            f"<td>{_system_config_value_cell(key, value, key in editable_keys)}</td>"
            f"<td>{escape(description)}</td>"
            "</tr>"
            for key, value, description in _system_config_rows()
        ],
    ]
    return (
        "<section class=\"card\">"
        "<h2>服务运行参数</h2>"
        "<p class=\"muted\">这些值来自环境变量或代码常量，控制服务怎么连接、读取和运行；"
        "不属于 Recipe 主规则，也不会写入 Developer Prompt 的 &lt;vars&gt;。"
        f"保存位置：<code>{escape(str(env_file_path()))}</code></p>"
        "<form method=\"post\" action=\"/config/system\">"
        "<table class=\"system-config-table\">"
        + "".join(rows)
        + "</table>"
        "<p><button type=\"submit\">保存高级参数</button></p>"
        "</form>"
        f"{_runtime_identity_cache_html(db_path)}"
        "</section>"
    )


def _runtime_identity_cache_html(db_path: Path | None) -> str:
    configured_db_path = os.environ.get("CEO_WORKER_DB", "").strip()
    store_path = (
        db_path
        or (_expand_configured_path(configured_db_path) if configured_db_path else None)
    )
    current_user_id = ""
    if store_path is not None and store_path.exists():
        current_user_id = AutoReplyStore(store_path).get_current_user_id() or ""
    table = "".join(
        "<tr>"
        f"<td><code class=\"config-value\">{escape(key)}</code></td>"
        f"<td><code class=\"config-value\">{escape(value)}</code></td>"
        f"<td>{escape(description)}</td>"
        "</tr>"
        for key, value, description in [
            (
                "current_user_id",
                current_user_id or "not cached",
                "DWS 当前登录账号写入 DB 的只读缓存；用于识别本人消息，不从 .env 手填。",
            )
        ]
    )
    return (
        "<h3>运行时身份缓存</h3>"
        "<p class=\"muted\">只展示本人身份真值；消息字段和组织字段不在这里配置。</p>"
        "<table class=\"system-config-table\">"
        "<tr><th>配置项</th><th>当前值</th><th>用途</th></tr>"
        f"{table}</table>"
    )


def _editable_system_config_keys() -> set[str]:
    return {
        "CEO_PRINCIPAL_NAME",
        "USER_ALIAS",
        "MEMORY_CONNECTOR_USER_ID",
        "CEO_MENTION_ALIASES",
        "CEO_BROADCAST_MENTION_ALIASES",
        "CEO_SINGLE_CHAT_ONLY",
        "DOCUMENT_EXTRACTION_IDS",
        "CEO_ASSISTANT_SIGNATURE",
        "CEO_HANDOFF_ACK",
        "CEO_FEEDBACK_SPIKE_VERCEL_BASE_URL",
        "CEO_NOT_SEND_MESSAGE",
        "CEO_DRY_RUN",
        "CEO_WORKSPACE",
        "CEO_WORKER_DB",
        "CEO_CORPUS_DIR",
        "CEO_WORK_PROFILE_PATH",
        "CEO_FORBIDDEN_PATH_PREFIXES",
        "CEO_PRODUCER_INTERVAL_SECONDS",
        "CEO_CONSUMER_POLL_INTERVAL_SECONDS",
        "CEO_POLL_INTERVAL_SECONDS",
        "CEO_BATCH_SECONDS",
        "FAST_PATH_UNREAD_BACKOFF",
        "MESSAGE_RECOVERY_INTERVAL",
        "SINGLE_CHAT_READ_RECOVERY_WINDOW",
        "SINGLE_CHAT_READ_RECOVERY_LIMIT",
        *read_env_file().keys(),
    }


def _system_config_key_cell(key: str, editable: bool) -> str:
    if not editable:
        return f"<code class=\"config-value\">{escape(key)}</code>"
    return (
        f"<code class=\"config-value\">{escape(key)}</code>"
        f"<input type=\"hidden\" name=\"system_key\" value=\"{escape(key)}\">"
    )


def _system_config_value_cell(key: str, value: str, editable: bool) -> str:
    if not editable:
        return f"<code class=\"config-value\">{escape(value)}</code>"
    return (
        "<input class=\"config-value-input\" type=\"text\" "
        f"name=\"system_value\" value=\"{escape(value)}\" "
        f"aria-label=\"{escape(key)}\">"
    )


def _highlight_logic_text(text: str) -> str:
    highlighted = escape(text)
    terms = [
        _slash_label(mention_aliases()),
        _slash_label(broadcast_mention_aliases()),
        "list_unread_conversations(count=50)",
        "message_fast_path_checked_at",
        _duration_label(fast_path_unread_backoff_duration()),
        "read_unread_messages",
        "read_mentioned_messages",
        "addresses_principal",
        "seen_messages",
        "reply_tasks",
        _duration_label(message_recovery_interval()),
        _duration_label(single_chat_read_recovery_window()),
    ]
    for term in sorted({item for item in terms if item}, key=len, reverse=True):
        escaped_term = escape(term)
        highlighted = highlighted.replace(
            escaped_term,
            f"<code class=\"config-token\">{escaped_term}</code>",
        )
    return highlighted


def _config_logic_sections() -> list[tuple[str, list[tuple[str, str]]]]:
    mention_example = _slash_label(mention_aliases())
    broadcast_example = _slash_label(broadcast_mention_aliases())
    fast_path_rows = [
        (
            "入口",
            "每次 producer 运行都会调用 list_unread_conversations(count=50)。"
            "快路径首次扫描到未读会话后，会读取未读消息并写入 reply_tasks/pending，"
            f"但延迟 {_duration_label(fast_path_unread_backoff_duration())} 后才允许 consumer 领取；"
            "慢路径未到点时，会过滤早于 message_fast_path_checked_at 的会话。",
        ),
        (
            "读取",
            "快路径首次触发时使用 read_unread_messages 取得可审计的 trigger。producer 也会调用 "
            f"read_mentioned_messages 和广播 mention 查询，所以即使未读状态不完整，"
            f"也能找到 {mention_example}、{broadcast_example} 这类点名或广播消息。",
        ),
        (
            "输出",
            "候选消息会经过过滤、按 seen_messages 去重、检查过期窗口；"
            "之后要么作为通知/系统消息跳过，要么进入 reply_tasks。"
            "等待窗口结束时如果会话已不再未读，会记录 skipped；仍未读则进入 processing。",
        ),
    ]
    slow_path_rows = [
        (
            "周期",
            f"每 {_duration_label(message_recovery_interval())} 运行一次。",
        ),
        (
            "私聊恢复",
            "从本地 DB 加入最近 "
            f"{_duration_label(single_chat_read_recovery_window())} 内的私聊会话，最多 "
            f"{single_chat_read_recovery_limit()} 个。它会读取最近消息和未读消息，"
            "再处理 latest seen message 之后的新消息。",
        ),
        (
            "群聊恢复",
            "慢路径不从本地 seen_messages 主动恢复群聊。群聊只通过 "
            "read_mentioned_messages、广播 mention 查询，或当前未读会话中的明确点名进入候选。",
        ),
    ]
    group_rows = [
        (
            "触发",
            "群聊候选必须通过 addresses_principal："
            f"包含 {mention_example}，或包含 {broadcast_example} 这类广播别名。"
            "没有这些点名信息的群聊消息，快路径和慢路径都不会处理。",
        ),
        (
            "文档",
            "群聊文档卡片只有先满足上面的群聊触发规则，才会进入 agent 判断。"
            f"没有 {mention_example} 的普通群聊文档分享不会创建 reply task。",
        ),
        (
            "合并",
            "同一发送人的连续候选消息会先合并再入队，所以一个 reply_task "
            "可以代表一小段相关群聊消息。",
        ),
    ]
    direct_rows = [
        (
            "触发",
            f"私聊不要求 {mention_example}。经过未读/恢复选择和系统通知过滤后，"
            "最新一条剩余私聊消息会进入 agent 判断。",
        ),
        (
            "文档",
            "私聊文档会进入 agent 判断；不能因为文档卡片渲染成图片/链接卡片，"
            "就直接当作 no_reply。",
        ),
        (
            "系统过滤",
            "预过滤仍会跳过明确的系统/状态通知、本人消息、过期且已 seen 的消息，"
            "以及不可处理的渲染媒体。日历、OA 审批、会议纪要权限消息会绕过通用通知跳过逻辑，进入各自的专门处理器。",
        ),
    ]
    return [
        ("快路径", fast_path_rows),
        ("慢路径", slow_path_rows),
        ("群聊", group_rows),
        ("私聊", direct_rows),
    ]


def _csv_label(values: tuple[str, ...]) -> str:
    return ", ".join(values)


def _slash_label(values: tuple[str, ...]) -> str:
    return "/".join(values)


def _duration_label(value) -> str:
    total_seconds = int(value.total_seconds())
    if total_seconds % 3600 == 0:
        hours = total_seconds // 3600
        return f"{hours}h"
    if total_seconds % 60 == 0:
        minutes = total_seconds // 60
        return f"{minutes}m"
    return f"{total_seconds}s"


def _page_offset(page: int, limit: int | None) -> int:
    if limit is None:
        return 0
    return max(0, page - 1) * limit


def _page_count(total_count: int, limit: int | None) -> int:
    if limit is None or limit <= 0:
        return 1
    return max(1, (max(0, total_count) + limit - 1) // limit)


def _bounded_page(page: int, limit: int | None, total_count: int) -> int:
    return min(max(1, page), _page_count(total_count, limit))


def _history_type_filters(values: str | Iterable[str]) -> tuple[str, ...]:
    raw_values = [values] if isinstance(values, str) else list(values)
    selected: list[str] = []
    for raw_value in raw_values:
        for part in str(raw_value).split(","):
            cleaned = part.strip().lower()
            if cleaned in HISTORY_TYPE_FILTERS and cleaned not in selected:
                selected.append(cleaned)
    return tuple(selected)


def _history_type_filter_label(type_filters: tuple[str, ...]) -> str:
    if not type_filters:
        return "全部类型"
    return "类型：" + ", ".join(type_filters)


def _attempt_list_limit(value: int) -> int:
    return value if value in ATTEMPT_LIST_LIMIT_OPTIONS else DEFAULT_ATTEMPT_LIST_LIMIT


def _page_href(
    base_path: str,
    page: int,
    *,
    limit: int | None = None,
    type_filters: tuple[str, ...] = (),
    include_limit: bool = False,
) -> str:
    query: dict[str, str | list[str]] = {}
    if page > 1:
        query["page"] = str(page)
    if include_limit and limit is not None and limit != DEFAULT_ATTEMPT_LIST_LIMIT:
        query["limit"] = str(limit)
    if type_filters:
        query["type"] = list(type_filters)
    if not query:
        return base_path
    return f"{base_path}?{urlencode(query, doseq=True)}"


def _format_local_time(value: str, *, local_tz: tzinfo | None = None) -> str:
    raw = value.strip()
    if not raw:
        return ""
    local_timezone = local_tz or datetime.now().astimezone().tzinfo
    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.strptime(raw, DISPLAY_TIME_FORMAT)
        except ValueError:
            return value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(local_timezone).strftime(DISPLAY_TIME_FORMAT)


def _parse_utc_timestamp(value: str) -> datetime | None:
    raw = value.strip()
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.strptime(raw, DISPLAY_TIME_FORMAT)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _history_event_label(attempt: ReplyAttempt) -> str:
    calendar_status = attempt.calendar_response_status.strip().lower()
    if calendar_status == "accepted":
        return "已接受"
    if calendar_status == "tentative":
        return "暂定"
    if calendar_status == "declined":
        return "已拒绝"

    oa_action = attempt.oa_action.strip().lower()
    if oa_action in {"agree", "approve", "approved"}:
        return "已通过"
    if oa_action in {"comment", "commented"}:
        return "已评论"
    if oa_action in {"return", "returned"}:
        return "已退回"
    if oa_action in {"refuse", "reject", "rejected"}:
        return "已拒绝"

    status = attempt.send_status.strip().lower()
    if status == "sent":
        return "已发送"
    if status == "skipped":
        return "已跳过"
    if status in {"failed", "blocked"}:
        return "失败"
    if status == "dry_run":
        return "预演"
    return "处理中"


def _history_chart_payload(
    store: AutoReplyStore,
    *,
    hours: int = HISTORY_CHART_HOURS,
    now: datetime | None = None,
) -> dict[str, object]:
    local_tz = datetime.now().astimezone().tzinfo
    local_now = now.astimezone(local_tz) if now else datetime.now(local_tz)
    bucket_count = max(1, hours)
    first_bucket = local_now.replace(minute=0, second=0, microsecond=0) - timedelta(
        hours=bucket_count - 1
    )
    labels = [
        (first_bucket + timedelta(hours=index)).strftime("%m-%d %H:%M")
        for index in range(bucket_count)
    ]
    since_utc = first_bucket.astimezone(timezone.utc).strftime(DISPLAY_TIME_FORMAT)
    attempts = store.list_reply_attempts_since(since_utc)
    bucket_values: dict[str, list[int]] = {}
    label_indexes = {label: index for index, label in enumerate(labels)}
    for attempt in attempts:
        created_at = _parse_utc_timestamp(attempt.created_at)
        if created_at is None:
            continue
        local_bucket = created_at.astimezone(local_tz).replace(
            minute=0,
            second=0,
            microsecond=0,
        )
        label = local_bucket.strftime("%m-%d %H:%M")
        bucket_index = label_indexes.get(label)
        if bucket_index is None:
            continue
        event_label = _history_event_label(attempt)
        bucket_values.setdefault(event_label, [0] * bucket_count)[bucket_index] += 1
    series = [
        {
            "name": name,
            "type": "bar",
            "stack": "events",
            "data": bucket_values[name],
            "itemStyle": {"color": HISTORY_CHART_COLORS.get(name, "#5a5a5c")},
        }
        for name in HISTORY_CHART_COLORS
        if name in bucket_values
    ]
    return {
        "labels": labels,
        "series": series,
        "total": sum(sum(item["data"]) for item in series),
        "range": f"{labels[0]} - {labels[-1]}",
    }


def _render_history_chart(store: AutoReplyStore) -> str:
    payload = _history_chart_payload(store)
    if int(payload["total"]) <= 0:
        return (
            "<section class=\"card history-chart-card\">"
            "<div class=\"history-chart-head\">"
            "<div><h2 class=\"history-chart-title\">最近 24 小时事件</h2>"
            f"<div class=\"history-chart-subtitle\">{escape(str(payload['range']))}</div></div>"
            "<span class=\"pill\">0 条事件</span>"
            "</div><div class=\"history-chart-empty\">暂无事件</div></section>"
        )
    payload_json = json.dumps(payload, ensure_ascii=False)
    return (
        "<section class=\"card history-chart-card\">"
        "<div class=\"history-chart-head\">"
        "<div><h2 class=\"history-chart-title\">最近 24 小时事件</h2>"
        f"<div class=\"history-chart-subtitle\">{escape(str(payload['range']))}</div></div>"
        f"<span class=\"pill\">{int(payload['total'])} 条事件</span>"
        "</div>"
        "<div id=\"history-event-chart\" class=\"history-chart\" role=\"img\" "
        "aria-label=\"最近 24 小时事件数量堆叠柱状图\"></div>"
        "<script src=\"https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js\"></script>"
        "<script>"
        f"window.historyEventChartData = {payload_json};"
        """
(() => {
  const el = document.getElementById("history-event-chart");
  if (!el || !window.echarts) {
    return;
  }
  const historyEventChartData = window.historyEventChartData;
  const chart = echarts.init(el, null, {renderer: "canvas"});
  chart.setOption({
    animation: false,
    tooltip: {trigger: "axis", axisPointer: {type: "shadow"}},
    legend: {top: 0, left: 0, itemWidth: 10, itemHeight: 10, textStyle: {color: "#5a5a5c"}},
    grid: {left: 34, right: 12, top: 54, bottom: 32},
    xAxis: {
      type: "category",
      data: historyEventChartData.labels,
      axisTick: {show: false},
      axisLabel: {color: "#888888", fontSize: 11, hideOverlap: true}
    },
    yAxis: {
      type: "value",
      minInterval: 1,
      splitLine: {lineStyle: {color: "#ededed"}},
      axisLabel: {color: "#888888", fontSize: 11}
    },
    series: historyEventChartData.series
  });
  window.addEventListener("resize", () => chart.resize());
})();
"""
        "</script></section>"
    )


def _pagination_range(page: int, limit: int | None, total_count: int) -> str:
    if total_count <= 0:
        return "0-0"
    if limit is None or limit <= 0:
        return f"1-{total_count}"
    start = _page_offset(page, limit) + 1
    end = min(start + limit - 1, total_count)
    return f"{start}-{end}"


def _pagination_button(
    *,
    label_html: str,
    aria_label: str,
    href: str | None,
    arrow: bool = False,
) -> str:
    classes = "pagination-button"
    if href is None:
        classes += " is-disabled"
    if arrow:
        classes += " pagination-arrow"
    label = escape(aria_label)
    if href is None:
        return (
            f"<span class=\"{classes}\" aria-label=\"{label}\" title=\"{label}\">"
            f"{label_html}</span>"
        )
    return (
        f"<a class=\"{classes}\" href=\"{escape(href)}\" "
        f"aria-label=\"{label}\" title=\"{label}\">{label_html}</a>"
    )


def _history_page_window(page: int, page_count: int) -> list[int | None]:
    if page_count <= 7:
        return list(range(1, page_count + 1))
    pages: list[int | None] = [1]
    start = max(2, page - 1)
    end = min(page_count - 1, page + 1)
    if start > 2:
        pages.append(None)
    pages.extend(range(start, end + 1))
    if end < page_count - 1:
        pages.append(None)
    pages.append(page_count)
    return pages


def _history_page_button(
    *,
    base_path: str,
    page: int,
    current_page: int,
    limit: int | None,
    type_filters: tuple[str, ...],
) -> str:
    if page == current_page:
        return (
            f"<span class=\"history-page-link active\" aria-current=\"page\">"
            f"{page}</span>"
        )
    return (
        f"<a class=\"history-page-link\" href=\""
        f"{escape(_page_href(base_path, page, limit=limit, type_filters=type_filters, include_limit=True))}"
        f"\">{page}</a>"
    )


def _table_page_button(
    *,
    page: int,
    current_page: int,
    href: str,
) -> str:
    if page == current_page:
        return (
            "<span class=\"table-page-link active\" aria-current=\"page\">"
            f"{page}</span>"
        )
    return f"<a class=\"table-page-link\" href=\"{escape(href)}\">{page}</a>"


def _table_page_links(
    *,
    page: int,
    page_count: int,
    href_for_page,
) -> str:
    page = min(max(1, page), page_count)
    prev_href = None if page <= 1 else href_for_page(page - 1)
    next_href = None if page >= page_count else href_for_page(page + 1)
    prev_html = (
        "<span class=\"table-page-arrow disabled\" aria-label=\"上一页\">&lsaquo;</span>"
        if prev_href is None
        else f"<a class=\"table-page-arrow\" href=\"{escape(prev_href)}\" aria-label=\"上一页\">&lsaquo;</a>"
    )
    next_html = (
        "<span class=\"table-page-arrow disabled\" aria-label=\"下一页\">&rsaquo;</span>"
        if next_href is None
        else f"<a class=\"table-page-arrow\" href=\"{escape(next_href)}\" aria-label=\"下一页\">&rsaquo;</a>"
    )
    page_links = []
    for item in _history_page_window(page, page_count):
        if item is None:
            page_links.append("<span class=\"table-page-ellipsis\">...</span>")
        else:
            page_links.append(
                _table_page_button(
                    page=item,
                    current_page=page,
                    href=href_for_page(item),
                )
            )
    return (
        "<nav class=\"table-page-links\" aria-label=\"分页导航\">"
        f"{prev_html}{''.join(page_links)}{next_html}</nav>"
    )


def _table_toolbar(
    *,
    name: str,
    search_label: str,
    query: str,
    type_select_html: str,
    page_links_html: str,
    page_size_select_html: str,
    total_count: int,
    action: str | None = None,
    search_input_id: str | None = None,
    search_name: str | None = None,
    search_clear_id: str | None = None,
    left_prefix_html: str = "",
    total_id: str | None = None,
) -> str:
    live_search_attr = " data-live-search=\"server\"" if action else ""
    open_tag = (
        f"<form class=\"table-toolbar\" data-table-toolbar=\"{escape(name)}\""
        f"{live_search_attr} method=\"get\" action=\"{escape(action)}\">"
        if action
        else f"<div class=\"table-toolbar\" data-table-toolbar=\"{escape(name)}\">"
    )
    close_tag = "</form>" if action else "</div>"
    input_attrs = [
        "type=\"text\"",
        "data-live-search-input",
        f"value=\"{escape(query.strip())}\"",
        "placeholder=\"搜索\"",
        "autocomplete=\"off\"",
    ]
    if search_input_id:
        input_attrs.insert(0, f"id=\"{escape(search_input_id)}\"")
    if search_name:
        input_attrs.insert(1, f"name=\"{escape(search_name)}\"")
    clear_attrs = [
        "class=\"table-search-clear\"",
        "type=\"button\"",
        "data-live-search-clear",
        "aria-label=\"清空搜索\"",
    ]
    if search_clear_id:
        clear_attrs.insert(0, f"id=\"{escape(search_clear_id)}\"")
    if not query.strip():
        clear_attrs.append("hidden")
    total_attrs = "class=\"table-toolbar-total\""
    if total_id:
        total_attrs = f"id=\"{escape(total_id)}\" {total_attrs}"
    toolbar_html = "".join(
        [
            open_tag,
            "<div class=\"table-toolbar-left\">",
            left_prefix_html,
            "<label class=\"table-toolbar-search\">",
            f"<span class=\"sr-only\">{escape(search_label)}</span>",
            f"<input {' '.join(input_attrs)}>",
            f"<button {' '.join(clear_attrs)}>×</button>",
            "</label>",
            type_select_html,
            "</div>",
            f"<div class=\"table-toolbar-center\">{page_links_html}</div>",
            "<div class=\"table-toolbar-right\">",
            page_size_select_html,
            f"<span {total_attrs}>共 {total_count} 条</span>",
            "</div>",
            close_tag,
        ]
    )
    if action:
        toolbar_html += _table_toolbar_live_search_script()
    return toolbar_html


def _table_toolbar_live_search_script() -> str:
    return """
<script data-table-toolbar-live-search>
(() => {
  document.querySelectorAll("form.table-toolbar[data-live-search='server']").forEach((form) => {
    const input = form.querySelector("[data-live-search-input]");
    if (!input) {
      return;
    }
    const toolbarName = form.getAttribute("data-table-toolbar");
    const clearButton = form.querySelector("[data-live-search-clear]");
    let timer = null;
    let requestId = 0;
    const submitSearch = async () => {
      const params = new URLSearchParams(new FormData(form));
      params.delete("page");
      if (input.name && !String(input.value || "").trim()) {
        params.delete(input.name);
      }
      Array.from(params.entries()).forEach(([key, value]) => {
        if (!value) {
          params.delete(key);
        }
      });
      const query = params.toString();
      const action = form.getAttribute("action") || window.location.pathname;
      const targetUrl = new URL(action, window.location.origin);
      targetUrl.search = query;
      const currentRequestId = ++requestId;
      const response = await fetch(targetUrl.toString(), {
        headers: {"X-Requested-With": "fetch"},
      });
      if (!response.ok || currentRequestId !== requestId) {
        return;
      }
      const nextDoc = new DOMParser().parseFromString(await response.text(), "text/html");
      const nextToolbar = nextDoc.querySelector(`[data-table-toolbar="${toolbarName}"]`);
      const currentRegion = document.querySelector(`[data-live-search-region="${toolbarName}"]`);
      const nextRegion = nextDoc.querySelector(`[data-live-search-region="${toolbarName}"]`);
      if (!nextToolbar || !currentRegion || !nextRegion) {
        return;
      }
      const nextCenter = nextToolbar.querySelector(".table-toolbar-center");
      const nextRight = nextToolbar.querySelector(".table-toolbar-right");
      const currentCenter = form.querySelector(".table-toolbar-center");
      const currentRight = form.querySelector(".table-toolbar-right");
      if (nextCenter && currentCenter) {
        currentCenter.innerHTML = nextCenter.innerHTML;
      }
      if (nextRight && currentRight) {
        currentRight.innerHTML = nextRight.innerHTML;
      }
      ["data-next-page", "data-has-more", "data-history-cursor"].forEach((name) => {
        if (nextRegion.hasAttribute(name)) {
          currentRegion.setAttribute(name, nextRegion.getAttribute(name) || "");
        }
      });
      currentRegion.innerHTML = nextRegion.innerHTML;
      history.replaceState(null, "", `${targetUrl.pathname}${targetUrl.search}`);
    };
    const scheduleSearch = () => {
      if (clearButton) {
        clearButton.hidden = !String(input.value || "").trim();
      }
      clearTimeout(timer);
      timer = setTimeout(submitSearch, 250);
    };
    input.addEventListener("input", scheduleSearch);
    if (clearButton) {
      clearButton.addEventListener("click", () => {
        input.value = "";
        submitSearch();
      });
    }
  });
})();
</script>
"""


def _history_table_header(
    *,
    base_path: str,
    page: int,
    limit: int | None,
    total_count: int,
    type_filters: tuple[str, ...],
    query: str = "",
) -> str:
    return _table_toolbar(
        name="history",
        action=base_path,
        search_label="搜索处理记录",
        search_name="q",
        query=query,
        type_select_html=_history_type_select(type_filters),
        page_links_html="",
        page_size_select_html="",
        total_count=total_count,
    )


def _history_page_href(
    *,
    base_path: str,
    page: int,
    limit: int | None,
    query: str,
    type_filters: tuple[str, ...],
) -> str:
    params: dict[str, str | list[str]] = {}
    if page > 1:
        params["page"] = str(page)
    if limit is not None and limit != DEFAULT_ATTEMPT_LIST_LIMIT:
        params["limit"] = str(limit)
    if query:
        params["q"] = query
    if type_filters:
        params["type"] = list(type_filters)
    if not params:
        return base_path
    return f"{base_path}?{urlencode(params, doseq=True)}"


def _history_type_select(type_filters: tuple[str, ...]) -> str:
    selected_value = type_filters[0] if len(type_filters) == 1 else ""
    all_label = _history_type_filter_label(type_filters)
    options = [
        f"<option value=\"\"{' selected' if not selected_value else ''}>{escape(all_label)}</option>"
    ]
    options.extend(
        f"<option value=\"{escape(value)}\"{' selected' if value == selected_value else ''}>"
        f"{escape(_history_type_display(value))}</option>"
        for value in HISTORY_TYPE_FILTERS
    )
    return (
        "<select name=\"type\" class=\"table-type-select\" "
        "aria-label=\"处理记录类型筛选\" onchange=\"this.form.submit()\">"
        f"{''.join(options)}</select>"
    )


def _history_type_display(value: str) -> str:
    return {
        "sent": "已发送",
        "reacted": "已表态",
        "skipped": "已跳过",
        "failed": "失败",
    }.get(value, value)


def _history_limit_select(limit: int | None) -> str:
    selected_limit = limit or DEFAULT_ATTEMPT_LIST_LIMIT
    option_values = sorted({*ATTEMPT_LIST_LIMIT_OPTIONS, selected_limit})
    options = "".join(
        f"<option value=\"{value}\"{' selected' if value == selected_limit else ''}>{value}/页</option>"
        for value in option_values
    )
    return (
        "<select class=\"table-page-size history-limit-select\" name=\"limit\" "
        "onchange=\"this.form.submit()\">"
        f"{options}</select>"
    )


def _pagination_controls(
    *,
    base_path: str,
    page: int,
    limit: int | None,
    total_count: int,
    bottom: bool = False,
    type_filters: tuple[str, ...] = (),
    include_limit: bool = False,
) -> str:
    page_count = _page_count(total_count, limit)
    if page_count <= 1:
        return ""
    page = min(max(1, page), page_count)
    is_first = page <= 1
    is_last = page >= page_count
    first_html = _pagination_button(
        label_html="首页",
        aria_label="第一页",
        href=None
        if is_first
        else _page_href(
            base_path,
            1,
            limit=limit,
            type_filters=type_filters,
            include_limit=include_limit,
        ),
    )
    prev_html = _pagination_button(
        label_html="&lsaquo;",
        aria_label="上一页",
        href=None
        if is_first
        else _page_href(
            base_path,
            page - 1,
            limit=limit,
            type_filters=type_filters,
            include_limit=include_limit,
        ),
        arrow=True,
    )
    next_html = _pagination_button(
        label_html="&rsaquo;",
        aria_label="下一页",
        href=None
        if is_last
        else _page_href(
            base_path,
            page + 1,
            limit=limit,
            type_filters=type_filters,
            include_limit=include_limit,
        ),
        arrow=True,
    )
    last_html = _pagination_button(
        label_html="末页",
        aria_label="最后一页",
        href=None
        if is_last
        else _page_href(
            base_path,
            page_count,
            limit=limit,
            type_filters=type_filters,
            include_limit=include_limit,
        ),
    )
    bottom_class = " bottom" if bottom else ""
    return (
        f"<div class=\"pagination{bottom_class}\">"
        "<div class=\"pagination-status\">"
        f"<span class=\"pagination-range\">{_pagination_range(page, limit, total_count)}</span>"
        f"<span class=\"pagination-page\">{page} / {page_count}</span>"
        f"<span class=\"pagination-total\">共 {total_count} 条</span>"
        "</div>"
        f"<nav class=\"pagination-actions\" aria-label=\"分页导航\">"
        f"{first_html}{prev_html}{next_html}{last_html}</nav>"
        "</div>"
    )


def _history_feed_region(
    store: AutoReplyStore,
    *,
    limit: int | None,
    page: int,
    type_filters: tuple[str, ...],
    query: str,
    total_count: int,
) -> str:
    items = _history_feed_items(
        store,
        limit=limit,
        page=page,
        type_filters=type_filters,
        query=query,
    )
    page_count = _page_count(total_count, limit)
    has_more = page < page_count
    next_page = page + 1 if has_more else ""
    has_more_attr = "1" if has_more else "0"
    cursor = _history_cursor(total_count, items)
    if not items:
        return (
            f"<div data-live-search-region=\"history\" data-infinite-list=\"history\" "
            f"data-next-page=\"{escape(str(next_page))}\" data-has-more=\"{has_more_attr}\" "
            f"data-history-cursor=\"{cursor}\">"
            "<section class=\"card\"><p class=\"muted\">暂无处理记录。</p>"
            f"<p class=\"muted\">DB: {escape(str(store.path))}</p></section>"
            f"{_infinite_load_status(has_more=has_more)}"
            "</div>"
        )
    items_html = "".join(items)
    return (
        f"<div data-live-search-region=\"history\" data-infinite-list=\"history\" "
        f"data-next-page=\"{escape(str(next_page))}\" data-has-more=\"{has_more_attr}\" "
        f"data-history-cursor=\"{cursor}\">"
        "<section class=\"attempt-feed\" data-infinite-items>"
        + items_html
        + "</section>"
        + _infinite_load_status(has_more=has_more)
        + "</div>"
    )


def _history_feed_items(
    store: AutoReplyStore,
    *,
    limit: int | None,
    page: int,
    type_filters: tuple[str, ...],
    query: str,
) -> list[str]:
    send_status_filters = type_filters or None
    offset = _page_offset(page, limit)
    items = []
    if page == 1 and not type_filters and not query:
        for task in store.list_reply_tasks(
            statuses=("pending", "processing"),
            limit=limit,
        ):
            items.append(_reply_task_item(task))
    attempts = store.list_reply_attempts(
        limit=limit,
        offset=offset,
        send_statuses=send_status_filters,
        query_text=query,
    )
    sent_replies_by_attempt = store.list_sent_replies_for_attempts(attempts)
    feedback_events_by_token = _feedback_events_by_sent_reply(
        store,
        sent_replies_by_attempt.values(),
    )
    for attempt in attempts:
        sent_reply = sent_replies_by_attempt.get(
            (attempt.conversation_id, attempt.trigger_message_id)
        )
        feedback_events = _feedback_events_for_sent_reply(
            sent_reply, feedback_events_by_token
        )
        warning_text = _attempt_warning_summary(attempt)
        warning_html = (
            f"<span class=\"attempt-warning\">{escape(warning_text)}</span>"
            if warning_text
            else ""
        )
        info_html = _attempt_info_icon(attempt)
        foot_section = (
            f'<div class="attempt-foot">{warning_html}</div>' if warning_html else ""
        )
        items.append(
            f"<article class=\"attempt-item\" data-href=\"/attempts/{attempt.id}\" "
            "tabindex=\"0\" role=\"link\">"
            "<div class=\"attempt-head\">"
            "<div class=\"attempt-title\">"
            f"<a class=\"attempt-id\" href=\"/attempts/{attempt.id}\">#{attempt.id}</a>"
            f"{info_html}"
            f"{_attempt_action_pills(attempt)}"
            f"<div class=\"attempt-main\">{escape(attempt.conversation_title)}</div>"
            f"<div class=\"attempt-meta\">{escape(attempt.trigger_sender)}</div>"
            "</div>"
            "<div class=\"attempt-side\">"
            f"<time class=\"attempt-time\">{escape(_format_local_time(attempt.created_at))}</time>"
            "<div class=\"attempt-actions\">"
            f"{_review_link(attempt)}"
            "</div>"
            "</div>"
            "</div>"
            "<div class=\"attempt-lines\">"
            f"{_attempt_text_line('问', attempt.trigger_text, 260)}"
            f"{_attempt_reply_line(attempt)}"
            "</div>"
            f"{_attempt_feedback_summary(feedback_events, sent_reply)}"
            f"{foot_section}"
            "</article>"
        )
    return items


def _infinite_load_status(*, has_more: bool) -> str:
    if has_more:
        return (
            "<div class=\"infinite-load-status\" data-infinite-status>"
            "继续滚动加载更多</div>"
        )
    return (
        "<div class=\"infinite-load-status done\" data-infinite-status>"
        "已加载全部</div>"
    )


def _history_cursor(total_count: int, items: list[str]) -> str:
    digest = hashlib.sha256()
    digest.update(str(total_count).encode("utf-8"))
    digest.update(b"\0")
    for item in items:
        digest.update(item.encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def _history_poll_script() -> str:
    return """
<script data-history-poll>
(() => {
  const region = document.querySelector('[data-live-search-region="history"]');
  if (!region) {
    return;
  }
  let cursor = region.getAttribute("data-history-cursor") || "";
  let inFlight = false;
  const pollMs = 1000;

  async function pollHistory() {
    if (document.hidden || inFlight) {
      return;
    }
    inFlight = true;
    try {
      const url = new URL("/api/history/updates", window.location.origin);
      const params = new URLSearchParams(window.location.search);
      if (cursor) {
        params.set("cursor", cursor);
      }
      url.search = params.toString();
      const response = await fetch(url.toString(), {
        cache: "no-store",
        headers: {"Accept": "application/json"},
      });
      if (!response.ok) {
        return;
      }
      const data = await response.json();
      if (!data.changed) {
        if (data.cursor) {
          cursor = data.cursor;
          region.setAttribute("data-history-cursor", cursor);
        }
        return;
      }
      const nextDoc = new DOMParser().parseFromString(data.region_html || "", "text/html");
      const nextRegion = nextDoc.querySelector('[data-live-search-region="history"]');
      if (!nextRegion) {
        return;
      }
      region.innerHTML = nextRegion.innerHTML;
      ["data-next-page", "data-has-more"].forEach((name) => {
        if (nextRegion.hasAttribute(name)) {
          region.setAttribute(name, nextRegion.getAttribute(name) || "");
        }
      });
      cursor = data.cursor || nextRegion.getAttribute("data-history-cursor") || "";
      region.setAttribute("data-history-cursor", cursor);
    } finally {
      inFlight = false;
    }
  }

  window.setInterval(pollHistory, pollMs);
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) {
      pollHistory();
    }
  });
})();
</script>
"""


def _clickable_attempt_cards_script() -> str:
    return """
<script data-clickable-attempt-cards>
(() => {
  const interactiveSelector = "a, button, input, select, textarea, summary, [role='button'], [data-no-card-click]";
  const openCard = (card, event) => {
    const href = card.getAttribute("data-href");
    if (!href) {
      return;
    }
    if (event.target && event.target.closest(interactiveSelector)) {
      return;
    }
    window.location.href = href;
  };
  document.addEventListener("click", (event) => {
    const card = event.target && event.target.closest(".attempt-item[data-href]");
    if (!card) {
      return;
    }
    openCard(card, event);
  });
  document.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") {
      return;
    }
    const card = event.target && event.target.closest(".attempt-item[data-href]");
    if (!card || event.target !== card) {
      return;
    }
    event.preventDefault();
    openCard(card, event);
  });
})();
</script>
"""


def _clickable_codex_session_rows_script() -> str:
    return """
<script data-clickable-codex-session-rows>
(() => {
  const interactiveSelector = "a, button, input, select, textarea, summary, [role='button'], [data-no-row-click]";
  const openRow = (row, event) => {
    const href = row.getAttribute("data-session-href");
    if (!href) {
      return;
    }
    if (event.target && event.target.closest(interactiveSelector)) {
      return;
    }
    window.location.assign(href);
  };
  document.addEventListener("click", (event) => {
    const row = event.target && event.target.closest(".codex-session-table tr[data-session-href]");
    if (!row) {
      return;
    }
    openRow(row, event);
  });
  document.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") {
      return;
    }
    const row = event.target && event.target.closest(".codex-session-table tr[data-session-href]");
    if (!row || event.target !== row) {
      return;
    }
    event.preventDefault();
    openRow(row, event);
  });
})();
</script>
"""


def _infinite_list_script() -> str:
    return """
<script data-infinite-list-loader>
(() => {
  document.querySelectorAll("[data-infinite-list]").forEach((region) => {
    const listName = region.getAttribute("data-infinite-list");
    if (!listName || region.dataset.infiniteReady === "1") {
      return;
    }
    region.dataset.infiniteReady = "1";
    let inFlight = false;
    const endpoint = listName === "logs" ? "/api/logs/page" : "/api/history/page";

    const updateStatus = (text, done = false) => {
      const statusEl = region.querySelector("[data-infinite-status]");
      if (!statusEl) {
        return;
      }
      statusEl.textContent = text;
      statusEl.classList.toggle("done", done);
    };
    const loadNextPage = async () => {
      if (inFlight || region.getAttribute("data-has-more") !== "1") {
        return;
      }
      const nextPage = Number(region.getAttribute("data-next-page") || "0");
      if (!nextPage) {
        return;
      }
      const itemsEl = region.querySelector("[data-infinite-items]");
      if (!itemsEl) {
        return;
      }
      inFlight = true;
      updateStatus("正在加载...");
      try {
        const url = new URL(endpoint, window.location.origin);
        const params = new URLSearchParams(window.location.search);
        params.set("page", String(nextPage));
        url.search = params.toString();
        const response = await fetch(url.toString(), {
          cache: "no-store",
          headers: {"Accept": "application/json"},
        });
        if (!response.ok) {
          updateStatus("加载失败，滚动到底部重试");
          return;
        }
        const data = await response.json();
        if (data.items_html) {
          itemsEl.insertAdjacentHTML("beforeend", data.items_html);
        }
        region.setAttribute("data-next-page", data.next_page || "");
        region.setAttribute("data-has-more", data.has_more ? "1" : "0");
        updateStatus(data.has_more ? "继续滚动加载更多" : "已加载全部", !data.has_more);
      } finally {
        inFlight = false;
      }
    };
    const nearBottom = () => window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 360;
    const onScroll = () => {
      if (nearBottom()) {
        loadNextPage();
      }
    };
    window.addEventListener("scroll", onScroll, {passive: true});
    onScroll();
  });
})();
</script>
"""


def render_history_page_chunk(
    store: AutoReplyStore,
    *,
    limit: int | None = DEFAULT_ATTEMPT_LIST_LIMIT,
    page: int = 1,
    type_filter: str | Iterable[str] = (),
    query: str = "",
) -> dict[str, object]:
    query = query.strip()
    type_filters = _history_type_filters(type_filter)
    total_count = store.count_reply_attempts(
        send_statuses=type_filters or None,
        query_text=query,
    )
    page = _bounded_page(page, limit, total_count)
    items = _history_feed_items(
        store,
        limit=limit,
        page=page,
        type_filters=type_filters,
        query=query,
    )
    page_count = _page_count(total_count, limit)
    has_more = page < page_count
    return {
        "items_html": "".join(items),
        "page": page,
        "next_page": page + 1 if has_more else None,
        "has_more": has_more,
        "total_count": total_count,
    }


def render_history_updates(
    store: AutoReplyStore,
    *,
    cursor: str = "",
    limit: int | None = DEFAULT_ATTEMPT_LIST_LIMIT,
    page: int = 1,
    type_filter: str | Iterable[str] = (),
    query: str = "",
) -> dict[str, object]:
    query = query.strip()
    type_filters = _history_type_filters(type_filter)
    total_count = store.count_reply_attempts(
        send_statuses=type_filters or None,
        query_text=query,
    )
    region_html = _history_feed_region(
        store,
        limit=limit,
        page=page,
        type_filters=type_filters,
        query=query,
        total_count=total_count,
    )
    marker = 'data-history-cursor="'
    next_cursor = region_html.split(marker, 1)[1].split('"', 1)[0]
    return {
        "changed": next_cursor != cursor,
        "cursor": next_cursor,
        "region_html": region_html if next_cursor != cursor else "",
        "total_count": total_count,
    }


def render_attempt_list(
    store: AutoReplyStore,
    limit: int | None = DEFAULT_ATTEMPT_LIST_LIMIT,
    page: int = 1,
    type_filter: str | Iterable[str] = (),
    query: str = "",
) -> str:
    query = query.strip()
    type_filters = _history_type_filters(type_filter)
    total_count = store.count_reply_attempts(
        send_statuses=type_filters or None,
        query_text=query,
    )
    page = _bounded_page(page, limit, total_count)
    header = _history_table_header(
        base_path="/",
        page=page,
        limit=limit,
        total_count=total_count,
        type_filters=type_filters,
        query=query,
    )
    body = (
        f"{_render_history_chart(store)}"
        f"{header}"
        f"{_history_feed_region(store, limit=limit, page=page, type_filters=type_filters, query=query, total_count=total_count)}"
        f"{_clickable_attempt_cards_script()}"
        f"{_infinite_list_script()}"
        f"{_history_poll_script()}"
    )
    return render_page(
        "一人 CEO 工作台",
        body,
        auto_refresh=False,
        active_nav="history",
        user_feedback_pending_count=store.count_pending_user_feedback_items(),
    )


def render_tasks_page(
    store: AutoReplyStore,
    query: str = "",
    category: str = "",
    task_state: str = "",
    sort: str = "",
    page: int = 1,
    page_size: int = DEFAULT_TASK_PAGE_SIZE,
) -> str:
    page_size = DEFAULT_TASK_PAGE_SIZE
    page_data = _task_page_data(
        store,
        query=query,
        category=category,
        task_state=task_state,
        sort=sort,
        page=page,
        page_size=page_size,
    )
    rows = page_data["rows"]
    categories = page_data["categories"]
    task_states = page_data["task_states"]
    initial_state = page_data["initial_state"]
    toolbar = _task_toolbar(
        total_count=int(page_data["total_count"]),
        query=query,
        category=initial_state["category"],
        categories=categories,
        page_size=initial_state["pageSize"],
    )
    body = (
        "<section class=\"tasks-page\">"
        f"{toolbar}"
        "<div id=\"tasks-table\" class=\"tasks-table-component\" "
        f"data-next-page=\"{escape(str(page_data['next_page'] or ''))}\" "
        f"data-has-more=\"{'1' if page_data['has_more'] else '0'}\">"
        "<div class=\"tasks-table-scroll\">"
        "<table class=\"tasks-table\" aria-label=\"任务列表\">"
        "<colgroup>"
        "<col style=\"width:230px\">"
        "<col style=\"width:118px\">"
        "<col style=\"width:94px\">"
        "<col style=\"width:110px\">"
        "<col style=\"width:260px\">"
        "<col style=\"width:300px\">"
        "<col style=\"width:132px\">"
        "<col style=\"width:296px\">"
        "</colgroup>"
        "<thead><tr>"
        f"{_task_sort_header('project', '任务', initial_state['sort'])}"
        f"{_task_sort_header('status', '状态', initial_state['sort'])}"
        f"{_task_sort_header('priority', '优先级', initial_state['sort'])}"
        f"{_task_sort_header('owner', 'Owner', initial_state['sort'])}"
        f"{_task_sort_header('state', '当前状态', initial_state['sort'])}"
        f"{_task_sort_header('next', '下一步', initial_state['sort'])}"
        f"{_task_sort_header('progress', '进度', initial_state['sort'])}"
        f"{_task_sort_header('todos', 'TODO', initial_state['sort'])}"
        "</tr></thead>"
        "<tbody id=\"tasks-table-body\"></tbody>"
        "</table>"
        "</div>"
        "</div>"
        "<div id=\"tasks-load-status\" class=\"infinite-load-status\" data-tasks-load-status>"
        f"{'继续滚动加载更多' if page_data['has_more'] else '已加载全部'}"
        "</div>"
        f"<script id=\"tasks-data\" type=\"application/json\">{_json_script_payload(rows)}</script>"
        f"<script id=\"tasks-initial-state\" type=\"application/json\">{_json_script_payload(initial_state)}</script>"
        f"<script id=\"tasks-categories\" type=\"application/json\">{_json_script_payload(categories)}</script>"
        f"<script id=\"tasks-states\" type=\"application/json\">{_json_script_payload(task_states)}</script>"
        f"{_task_table_script()}"
        "</section>"
    )
    return render_page(
        "任务",
        body,
        active_nav="tasks",
        user_feedback_pending_count=store.count_pending_user_feedback_items(),
        main_class="main-wide tasks-main",
    )


def _task_sort_header(field: str, label: str, current_sort: str) -> str:
    current = str(current_sort or "")
    active = current.startswith(f"{field}_")
    direction = "asc" if current.endswith("_desc") else "desc"
    icon = "↑" if current.endswith("_asc") else "↓" if current.endswith("_desc") else "↕"
    active_class = " active" if active else ""
    aria_sort = "none"
    if active:
        aria_sort = "ascending" if current.endswith("_asc") else "descending"
    return (
        f"<th scope=\"col\" aria-sort=\"{aria_sort}\">"
        f"<button type=\"button\" class=\"tasks-table-sort{active_class}\" "
        f"data-task-sort-field=\"{escape(field)}\" data-task-sort-next=\"{escape(field)}_{direction}\">"
        f"<span>{escape(label)}</span>"
        f"<span class=\"tasks-table-sort-icon\" aria-hidden=\"true\">{escape(icon)}</span>"
        "</button>"
        "</th>"
    )


def _task_page_data(
    store: AutoReplyStore,
    *,
    query: str,
    category: str,
    task_state: str,
    sort: str,
    page: int,
    page_size: int,
) -> dict[str, object]:
    projects = store.list_work_projects(limit=500)
    items = [
        (project, store.list_work_todos(project_id=project.id))
        for project in projects
    ]
    categories = _task_categories(items)
    task_states = _task_states(items)
    all_rows = [_task_row_payload(project, todos) for project, todos in items]
    rows = _filter_task_rows(
        all_rows,
        query=query,
        category=category,
        task_state=task_state,
    )
    rows = _sort_task_rows(rows, sort=sort)
    total_count = len(rows)
    page = _bounded_page(page, page_size, total_count)
    offset = _page_offset(page, page_size)
    page_rows = rows[offset : offset + page_size]
    page_count = _page_count(total_count, page_size)
    has_more = page < page_count
    initial_state = {
        "query": query.strip(),
        "category": category.strip(),
        "taskState": task_state.strip(),
        "sort": _bounded_task_sort(sort),
        "page": max(page, 1),
        "pageSize": page_size,
        "totalCount": total_count,
    }
    return {
        "rows": page_rows,
        "categories": categories,
        "task_states": task_states,
        "initial_state": initial_state,
        "total_count": total_count,
        "page": page,
        "next_page": page + 1 if has_more else None,
        "has_more": has_more,
    }


def _filter_task_rows(
    rows: list[dict],
    *,
    query: str,
    category: str,
    task_state: str,
) -> list[dict]:
    terms = [term.casefold() for term in query.strip().split() if term.strip()]
    category = category.strip()
    task_state = task_state.strip()
    result = []
    for row in rows:
        if category and str(row.get("category", "")) != category:
            continue
        if task_state and str(row.get("status", "")) != task_state:
            continue
        search = str(row.get("search", ""))
        if terms and not all(term in search for term in terms):
            continue
        result.append(row)
    return result


def _sort_task_rows(rows: list[dict], *, sort: str) -> list[dict]:
    field, direction = _task_sort_options().get(_bounded_task_sort(sort), ("", ""))
    if not field:
        return rows
    reverse = direction == "desc"
    return sorted(
        rows,
        key=lambda row: "" if row.get(field) is None else row.get(field),
        reverse=reverse,
    )


def render_task_page_chunk(
    store: AutoReplyStore,
    *,
    query: str = "",
    category: str = "",
    task_state: str = "",
    sort: str = "",
    page: int = 1,
    page_size: int = DEFAULT_TASK_PAGE_SIZE,
) -> dict[str, object]:
    data = _task_page_data(
        store,
        query=query,
        category=category,
        task_state=task_state,
        sort=sort,
        page=page,
        page_size=DEFAULT_TASK_PAGE_SIZE,
    )
    return {
        "rows": data["rows"],
        "page": data["page"],
        "next_page": data["next_page"],
        "has_more": data["has_more"],
        "total_count": data["total_count"],
        "categories": data["categories"],
        "task_states": data["task_states"],
    }


def _json_script_payload(value) -> str:
    return json.dumps(value, ensure_ascii=False).replace("</", "<\\/")


def _task_row_payload(project, todos) -> dict:
    open_count, open_ratio = _task_open_summary(todos)
    progress_count, progress_ratio = _task_progress_summary(todos)
    state = _task_table_state(project, todos)
    todo_payloads = []
    for todo in todos:
        due = _format_local_time(todo.deadline_at) or todo.deadline_at
        todo_payloads.append(
            {
                "title": todo.title,
                "owner": todo.owner_name,
                "status": str(todo.status),
                "done": _task_todo_done(todo),
                "due": due,
            }
        )
    return {
        "id": project.id,
        "title": project.title,
        "detailUrl": f"/tasks/{project.id}",
        "status": state,
        "statusRank": _task_state_sort_rank().get(state, 99),
        "category": str(project.category),
        "priority": str(project.priority),
        "priorityRank": _task_priority_sort_rank().get(str(project.priority), 99),
        "riskLevel": str(project.risk_level),
        "riskRank": _task_risk_sort_rank().get(str(project.risk_level), 99),
        "owner": project.owner_name,
        "currentState": _excerpt(project.current_state, 120),
        "nextStep": _excerpt(project.next_step, 140),
        "openCount": open_count,
        "openRatio": open_ratio,
        "openSummary": f"{open_count} ({open_ratio}%)",
        "progressCount": progress_count,
        "progressTotal": len(todos),
        "progressRatio": progress_ratio,
        "progressSummary": f"{progress_count}/{len(todos)} ({progress_ratio}%)",
        "todoCount": len(todos),
        "todos": todo_payloads,
        "search": "\n".join(_task_project_search_values(project, todos)).casefold(),
    }


def _task_priority_sort_rank() -> dict[str, int]:
    return {
        ProjectPriority.P0.value: 0,
        ProjectPriority.P1.value: 1,
        ProjectPriority.P2.value: 2,
        ProjectPriority.NONE.value: 3,
    }


def _task_risk_sort_rank() -> dict[str, int]:
    return {
        RiskLevel.HIGH.value: 0,
        RiskLevel.MEDIUM.value: 1,
        RiskLevel.LOW.value: 2,
        RiskLevel.NONE.value: 3,
    }


def _bounded_task_page_size(page_size: int) -> int:
    return page_size if page_size in TASK_PAGE_SIZE_OPTIONS else DEFAULT_TASK_PAGE_SIZE


def _bounded_log_page_size(page_size: int) -> int:
    return page_size if page_size in LOG_PAGE_SIZE_OPTIONS else DEFAULT_ERROR_LIST_LIMIT


def _task_categories(items) -> list[str]:
    return sorted({str(project.category) for project, _todos in items if str(project.category)})


def _task_states(items) -> list[str]:
    return sorted(
        {_task_table_state(project, todos) for project, todos in items},
        key=lambda value: _task_state_sort_rank().get(value, 99),
    )


def _bounded_task_sort(sort: str) -> str:
    return sort if sort in _task_sort_options() else ""


def _task_sort_options() -> dict[str, tuple[str, str]]:
    return {
        "": ("", ""),
        "project_desc": ("title", "desc"),
        "project_asc": ("title", "asc"),
        "status_desc": ("statusRank", "asc"),
        "status_asc": ("statusRank", "desc"),
        "priority_desc": ("priorityRank", "asc"),
        "priority_asc": ("priorityRank", "desc"),
        "risk_desc": ("riskRank", "asc"),
        "risk_asc": ("riskRank", "desc"),
        "owner_desc": ("owner", "desc"),
        "owner_asc": ("owner", "asc"),
        "state_desc": ("currentState", "desc"),
        "state_asc": ("currentState", "asc"),
        "next_desc": ("nextStep", "desc"),
        "next_asc": ("nextStep", "asc"),
        "open_desc": ("openCount", "desc"),
        "open_asc": ("openCount", "asc"),
        "progress_desc": ("progressRatio", "desc"),
        "progress_asc": ("progressRatio", "asc"),
        "todos_desc": ("todoCount", "desc"),
        "todos_asc": ("todoCount", "asc"),
    }


def _task_state_sort_rank() -> dict[str, int]:
    return {
        "over due": 0,
        "in progress": 1,
        "not started": 2,
        "completed": 3,
    }


def _task_open_summary(todos) -> tuple[int, int]:
    total = len(todos)
    open_count = sum(1 for todo in todos if _task_todo_incomplete(todo))
    if total <= 0:
        return open_count, 0
    return open_count, round(open_count * 100 / total)


def _task_progress_summary(todos) -> tuple[int, int]:
    total = len(todos)
    done_count = sum(1 for todo in todos if _task_todo_done(todo))
    if total <= 0:
        return done_count, 0
    return done_count, round(done_count * 100 / total)


def _task_todo_incomplete(todo) -> bool:
    return str(todo.status) not in {TodoStatus.DONE.value, TodoStatus.CANCELLED.value}


def _task_todo_done(todo) -> bool:
    return str(todo.status) == TodoStatus.DONE.value


def _task_table_state(project, todos) -> str:
    if str(project.status) == ProjectStatus.DONE.value:
        return "completed"
    if todos and not any(_task_todo_incomplete(todo) for todo in todos):
        return "completed"
    if any(_task_todo_overdue(todo) for todo in todos if _task_todo_incomplete(todo)):
        return "over due"
    if any(_task_todo_incomplete(todo) for todo in todos):
        return "in progress"
    return "not started"


def _task_todo_overdue(todo) -> bool:
    deadline = _parse_utc_timestamp(todo.deadline_at)
    return bool(deadline and deadline < datetime.now(timezone.utc))


def _task_toolbar(
    *,
    total_count: int,
    query: str,
    category: str,
    categories: list[str],
    page_size: int,
) -> str:
    query = query.strip()
    return _table_toolbar(
        name="tasks",
        search_label="搜索任务",
        query=query,
        search_input_id="task-search-input",
        search_clear_id="task-search-clear",
        left_prefix_html=(
            f"<span id=\"tasks-count\" class=\"tasks-count\">共 {total_count} 个任务</span>"
        ),
        type_select_html=_task_type_select(category=category, categories=categories),
        page_links_html="",
        page_size_select_html="",
        total_count=total_count,
        total_id="tasks-total",
    )


def _task_type_select(*, category: str, categories: list[str]) -> str:
    options = [f"<option value=\"\"{' selected' if not category else ''}>全部类型</option>"]
    options.extend(
        f"<option value=\"{escape(value)}\"{' selected' if value == category else ''}>"
        f"{escape(value)}</option>"
        for value in categories
    )
    return (
        "<select id=\"task-type-filter\" class=\"table-type-select\" "
        "aria-label=\"任务类型筛选\">"
        f"{''.join(options)}</select>"
    )


def _task_page_size_select(
    *,
    page_size: int,
) -> str:
    options = "".join(
        f"<option value=\"{size}\"{' selected' if size == page_size else ''}>{size}/页</option>"
        for size in TASK_PAGE_SIZE_OPTIONS
    )
    return (
        "<select id=\"task-page-size\" class=\"table-page-size tasks-page-size\" "
        "aria-label=\"每页任务数\">"
        f"{options}</select>"
    )


def _task_table_script() -> str:
    sort_options_json = _json_script_payload(_task_sort_options())
    return f"""
<script data-task-table>
(() => {{
  const rows = JSON.parse(document.getElementById("tasks-data").textContent || "[]");
  const initial = JSON.parse(document.getElementById("tasks-initial-state").textContent || "{{}}");
  const sortOptions = {sort_options_json};
  const countEl = document.getElementById("tasks-count");
  const totalEl = document.getElementById("tasks-total");
  const searchInput = document.getElementById("task-search-input");
  const clearButton = document.getElementById("task-search-clear");
  const typeFilter = document.getElementById("task-type-filter");
  const tableEl = document.getElementById("tasks-table");
  const tableBody = document.getElementById("tasks-table-body");
  const statusEl = document.getElementById("tasks-load-status");
  const sortButtons = Array.from(document.querySelectorAll("[data-task-sort-field]"));
  let nextPage = Number(tableEl.getAttribute("data-next-page") || "0");
  let hasMore = tableEl.getAttribute("data-has-more") === "1";
  let inFlight = false;
  let requestId = 0;
  let loadedRows = rows.slice();
  const checkIcon = `{_lucide_icon("check")}`;

  const escapeHtml = (value) => String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
  const pill = (value) => `<span class="pill">${{escapeHtml(value || "-")}}</span>`;
  const badge = (value) => {{
    const cssClass = String(value || "").replace(/\\s+/g, "-");
    return `<span class="task-state ${{escapeHtml(cssClass)}}">${{escapeHtml(value || "-")}}</span>`;
  }};
  const progressMarkup = (row) => {{
    const ratio = Math.max(0, Math.min(100, Number(row.progressRatio) || 0));
    return `<div class="progress-cell"><div class="progress-meter"><div class="progress-bar" style="width:${{ratio}}%"></div></div><div class="progress-label">${{escapeHtml(row.progressSummary)}}</div></div>`;
  }};
  const todoMarkup = (row) => {{
    const todos = row.todos || [];
    if (!todos.length) {{
      return `<span class="muted">-</span>`;
    }}
    const visibleTodos = todos.slice(0, 3);
    const items = visibleTodos.map((todo) => {{
      const checkClass = todo.done ? "todo-check done" : "todo-check";
      const check = todo.done ? checkIcon : "";
      const due = todo.due ? `<span class="todo-due">DDL ${{escapeHtml(todo.due)}}</span>` : "";
      return `<li><span class="${{checkClass}}" aria-hidden="true">${{check}}</span><span class="todo-copy"><span>${{escapeHtml(todo.title)}}</span>${{due}}</span></li>`;
    }});
    if (todos.length > visibleTodos.length) {{
      items.push(`<li class="todo-total">总共 ${{todos.length}} 条</li>`);
    }}
    return `<ul class="todo-checklist">${{items.join("")}}</ul>`;
  }};
  const taskRow = (row) => `
    <tr data-task-id="${{escapeHtml(row.id)}}" data-task-href="${{escapeHtml(row.detailUrl)}}" tabindex="0">
      <td>
        <a class="task-table-link" href="${{escapeHtml(row.detailUrl)}}">${{escapeHtml(row.title)}}</a>
        <div class="task-table-meta">${{pill(row.category)}}${{pill(row.riskLevel)}}</div>
      </td>
      <td>${{badge(row.status)}}</td>
      <td>${{pill(row.priority)}}</td>
      <td>${{escapeHtml(row.owner || "-")}}</td>
      <td><div class="task-table-text">${{escapeHtml(row.currentState || "-")}}</div></td>
      <td><div class="task-table-text">${{escapeHtml(row.nextStep || "-")}}</div></td>
      <td class="task-table-progress">${{progressMarkup(row)}}</td>
      <td>${{todoMarkup(row)}}</td>
    </tr>`;
  const renderRows = (nextRows, append = false) => {{
    if (!append) {{
      tableBody.innerHTML = "";
    }}
    if (!nextRows.length && !append) {{
      tableBody.innerHTML = '<tr><td colspan="8"><div class="task-table-empty">没有匹配的任务</div></td></tr>';
      return;
    }}
    tableBody.insertAdjacentHTML("beforeend", nextRows.map(taskRow).join(""));
  }};
  const isPlainRowActivation = (event) => {{
    return !event.target.closest("a,button,input,select,textarea,label,[role='button'],[data-custom-select]");
  }};
  const openTaskRow = (row) => {{
    const href = row?.getAttribute("data-task-href") || "";
    if (href) {{
      window.location.assign(href);
    }}
  }};
  const updateCount = (count) => {{
    const total = Number.isFinite(Number(count)) ? Number(count) : loadedRows.length;
    countEl.textContent = `共 ${{total}} 个任务`;
    totalEl.textContent = `共 ${{total}} 条`;
  }};
  const updateLoadState = () => {{
    tableEl.setAttribute("data-next-page", nextPage || "");
    tableEl.setAttribute("data-has-more", hasMore ? "1" : "0");
    if (statusEl) {{
      statusEl.textContent = hasMore ? "继续滚动加载更多" : "已加载全部";
      statusEl.classList.toggle("done", !hasMore);
    }}
  }};
  const updateSortButtons = () => {{
    sortButtons.forEach((button) => {{
      const field = button.getAttribute("data-task-sort-field") || "";
      const active = initial.sort === `${{field}}_asc` || initial.sort === `${{field}}_desc`;
      const nextDirection = initial.sort === `${{field}}_desc` ? "asc" : "desc";
      const icon = button.querySelector(".tasks-table-sort-icon");
      button.classList.toggle("active", active);
      button.setAttribute("data-task-sort-next", `${{field}}_${{nextDirection}}`);
      button.closest("th")?.setAttribute(
        "aria-sort",
        active ? (initial.sort.endsWith("_asc") ? "ascending" : "descending") : "none"
      );
      if (icon) {{
        icon.textContent = active ? (initial.sort.endsWith("_asc") ? "↑" : "↓") : "↕";
      }}
    }});
  }};
  const currentParams = (page) => {{
    const params = new URLSearchParams();
    const query = String(searchInput.value || "").trim();
    const category = String(typeFilter.value || "").trim();
    if (query) {{
      params.set("q", query);
    }}
    if (category) {{
      params.set("category", category);
    }}
    if (initial.taskState) {{
      params.set("task_state", initial.taskState);
    }}
    if (initial.sort) {{
      params.set("sort", initial.sort);
    }}
    params.set("page", String(page));
    return params;
  }};
  const fetchTaskPage = async (page, mode) => {{
    if (inFlight) {{
      return;
    }}
    inFlight = true;
    const currentRequestId = ++requestId;
    if (statusEl) {{
      statusEl.textContent = "正在加载...";
    }}
    try {{
      const url = new URL("/api/tasks/page", window.location.origin);
      url.search = currentParams(page).toString();
      const response = await fetch(url.toString(), {{
        cache: "no-store",
        headers: {{"Accept": "application/json"}},
      }});
      if (!response.ok || currentRequestId !== requestId) {{
        return;
      }}
      const data = await response.json();
      nextPage = Number(data.next_page || 0);
      hasMore = Boolean(data.has_more);
      if (mode === "replace") {{
        loadedRows = data.rows || [];
        renderRows(loadedRows, false);
        updateCount(data.total_count);
        history.replaceState(null, "", `${{window.location.pathname}}?${{currentParams(1).toString()}}`);
      }} else {{
        const nextRows = data.rows || [];
        loadedRows = loadedRows.concat(nextRows);
        renderRows(nextRows, true);
        updateCount(data.total_count);
      }}
      updateLoadState();
    }} finally {{
      inFlight = false;
    }}
  }};
  const nearBottom = () => window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 420;
  const loadNextIfNeeded = () => {{
    if (hasMore && nearBottom()) {{
      fetchTaskPage(nextPage, "append");
    }}
  }};

  if (initial.category) {{
    typeFilter.value = initial.category;
  }}
  if (initial.query) {{
    searchInput.value = initial.query;
  }} else {{
    clearButton.hidden = true;
  }}
  renderRows(loadedRows, false);
  updateCount(initial.totalCount || loadedRows.length);
  updateSortButtons();
  updateLoadState();
  loadNextIfNeeded();

  let searchTimer = null;
  searchInput.addEventListener("input", () => {{
    clearButton.hidden = !String(searchInput.value || "").trim();
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => fetchTaskPage(1, "replace"), 250);
  }});
  clearButton.addEventListener("click", () => {{
    searchInput.value = "";
    clearButton.hidden = true;
    fetchTaskPage(1, "replace");
    searchInput.focus();
  }});
  typeFilter.addEventListener("change", () => fetchTaskPage(1, "replace"));
  tableBody.addEventListener("click", (event) => {{
    if (!isPlainRowActivation(event)) {{
      return;
    }}
    const row = event.target.closest("tr[data-task-href]");
    openTaskRow(row);
  }});
  tableBody.addEventListener("keydown", (event) => {{
    if (event.key !== "Enter" && event.key !== " ") {{
      return;
    }}
    if (!isPlainRowActivation(event)) {{
      return;
    }}
    const row = event.target.closest("tr[data-task-href]");
    if (!row) {{
      return;
    }}
    event.preventDefault();
    openTaskRow(row);
  }});
  sortButtons.forEach((button) => {{
    button.addEventListener("click", () => {{
      initial.sort = button.getAttribute("data-task-sort-next") || "";
      updateSortButtons();
      fetchTaskPage(1, "replace");
    }});
  }});
  window.addEventListener("scroll", loadNextIfNeeded, {{passive: true}});
}})();
</script>
"""

def _task_project_search_values(project, todos) -> list[str]:
    values = [
        project.title,
        str(project.category),
        project.tags_json,
        str(project.status),
        str(project.priority),
        str(project.risk_level),
        project.owner_user_id,
        project.owner_name,
        project.related_people_json,
        project.goal,
        project.background,
        project.facts_json,
        project.current_state,
        project.blocker,
        project.next_step,
        project.source_conversations_json,
        project.memory_context_json,
    ]
    for todo in todos:
        values.extend(
            [
                todo.title,
                todo.owner_user_id,
                todo.owner_name,
                str(todo.status),
                str(todo.priority),
                todo.deadline_at,
                todo.next_follow_up_at,
                todo.follow_up_question,
                todo.blocker,
                todo.completion_evidence_json,
            ]
        )
    return [value for value in values if value]


def render_task_project_detail(store: AutoReplyStore, project_id: int) -> tuple[int, str]:
    project = store.get_work_project(project_id)
    if project is None:
        body = (
            f"{_detail_page_header('Project not found', back_href='/tasks', back_label='返回任务')}"
            "<section class=\"card\">"
            f"<p class=\"muted\">No work project exists for id {project_id}.</p>"
            "</section>"
        )
        return (
            404,
            render_page(
                "Task project",
                body,
                user_feedback_pending_count=store.count_pending_user_feedback_items(),
                show_nav=False,
                show_header=False,
            ),
        )

    todos = store.list_work_todos(project_id=project.id)
    updates = store.list_work_updates(project.id, limit=50)
    drafts = store.list_follow_up_drafts(project_id=project.id, limit=100)

    detail_rows = _task_project_detail_rows(project)
    facts = _task_facts_rows(project.facts_json)
    conversation_titles = _task_conversation_title_map(project.source_conversations_json)
    todo_panel = _task_todos_panel(todos, drafts, conversation_titles)
    update_rows = _task_update_rows(updates)
    draft_rows = _task_follow_up_rows(
        _unlinked_follow_up_drafts(todos, drafts),
        conversation_titles,
    )

    body = (
        f"{_detail_page_header(project.title, back_href='/tasks', back_label='返回任务')}"
        "<section class=\"card\">"
        "<div class=\"reply-meta\">"
        f"<span class=\"pill\">{escape(project.status)}</span>"
        f"<span class=\"pill\">{escape(project.category)}</span>"
        f"<span class=\"pill\">{escape(project.priority)}</span>"
        f"<span class=\"pill\">risk {escape(project.risk_level)}</span>"
        "</div>"
        "<div class=\"attempt-detail-grid\">"
        f"{_task_detail_cell('Owner', project.owner_name or project.owner_user_id or '-')}"
        f"{_task_detail_cell('Next follow-up', _format_local_time(project.next_follow_up_at) or '-')}"
        f"{_task_detail_cell('Updated', _format_local_time(project.updated_at))}"
        f"{_task_detail_cell('Derek attention', 'yes' if project.needs_derek_attention else 'no')}"
        "</div>"
        "</section>"
        "<section class=\"card\"><h2>Project details</h2>"
        f"{_task_project_detail_table(detail_rows)}"
        "</section>"
        "<section class=\"card\"><h2>TODOs</h2>"
        f"{todo_panel if todos else '<p class=\"muted\">No TODOs recorded.</p>'}"
        "</section>"
        "<section class=\"card\"><h2>Facts</h2>"
        f"{_simple_table(('Description', 'Source', 'Created', 'Updated'), facts, column_widths={'Source': '118px', 'Created': '132px', 'Updated': '132px'}) if facts else '<p class=\"muted\">No facts recorded.</p>'}"
        "</section>"
        "<section class=\"card\"><h2>Updates</h2>"
        f"{_simple_table(('Time', 'Source', 'Summary', 'Changes', 'Reason', 'Confidence'), update_rows, column_widths={'Time': '148px', 'Source': '118px', 'Summary': '240px', 'Changes': '220px', 'Reason': '180px', 'Confidence': '96px'}) if update_rows else '<p class=\"muted\">No updates recorded.</p>'}"
        "</section>"
        + (
            "<section class=\"card\"><h2>Unlinked follow-ups</h2>"
            f"{_simple_table(('Time', 'Owner', 'TODO', 'Target', 'Status', 'Question', 'Risk', 'Result'), draft_rows, column_widths={'Time': '148px', 'Owner': '110px', 'TODO': '88px', 'Target': '112px', 'Status': '104px', 'Question': '240px', 'Risk': '170px', 'Result': '180px'}, html_columns={'TODO'})}"
            "</section>"
            if draft_rows
            else ""
        )
        + f"{_collapsible_json_card('Memory context', project.memory_context_json)}"
    )
    return (
        200,
        render_page(
            project.title,
            body,
            user_feedback_pending_count=store.count_pending_user_feedback_items(),
            show_nav=False,
            show_header=False,
        ),
    )


def _task_project_detail_rows(project) -> list[tuple[str, str, bool]]:
    tags = _task_detail_pills(_task_simple_labels(project.tags_json))
    related_people = _task_detail_pills(_task_people_labels(project.related_people_json))
    source_conversations = _task_detail_pills(
        _task_conversation_labels(project.source_conversations_json)
    )
    return [
        ("Goal", project.goal, False),
        ("Background", project.background, False),
        ("Current state", project.current_state, False),
        ("Blocker", project.blocker, False),
        ("Next step", project.next_step, False),
        ("Follow-up mode", str(project.follow_up_mode), False),
        ("Tags", tags, True),
        ("Related people", related_people, True),
        ("Source conversations", source_conversations, True),
        ("Created", _format_local_time(project.created_at), False),
        ("Last activity", _format_local_time(project.last_activity_at), False),
    ]


def _task_project_detail_table(rows: Iterable[tuple[str, str, bool]]) -> str:
    row_html = "".join(
        "<tr>"
        f"<td>{escape(field)}</td>"
        f"<td>{value if is_html else escape(value)}</td>"
        "</tr>"
        for field, value, is_html in rows
    )
    return (
        "<table><thead><tr><th>Field</th><th>Value</th></tr></thead><tbody>"
        f"{row_html}"
        "</tbody></table>"
    )


def _task_detail_pills(labels: Iterable[str]) -> str:
    pills = "".join(
        f'<span class="detail-pill">{escape(label)}</span>'
        for label in labels
        if label
    )
    return f'<div class="detail-pill-list">{pills}</div>' if pills else "-"


def _task_simple_labels(text: str) -> list[str]:
    labels = []
    for item in _json_list(text):
        label = str(item).strip()
        if label:
            labels.append(label)
    return labels


def _task_people_labels(text: str) -> list[str]:
    labels = []
    for item in _json_list(text):
        if isinstance(item, dict):
            label = str(item.get("name") or item.get("user_id") or "").strip()
        else:
            label = str(item).strip()
        if label:
            labels.append(label)
    return labels


def _task_conversation_labels(text: str) -> list[str]:
    labels = []
    for item in _json_list(text):
        if isinstance(item, dict):
            label = str(item.get("title") or item.get("name") or "").strip()
            if not label:
                label = str(
                    item.get("conversation_id")
                    or item.get("id")
                    or item.get("open_conversation_id")
                    or ""
                ).strip()
        else:
            label = str(item).strip()
        if label:
            labels.append(label)
    return labels


def _task_conversation_title_map(text: str) -> dict[str, str]:
    titles = {}
    for item in _json_list(text):
        if not isinstance(item, dict):
            continue
        conversation_id = str(
            item.get("conversation_id")
            or item.get("id")
            or item.get("open_conversation_id")
            or ""
        ).strip()
        title = str(item.get("title") or item.get("name") or "").strip()
        if conversation_id and title:
            titles[conversation_id] = title
    return titles


def _task_facts_rows(facts_json: str) -> list[tuple[str, str, str, str]]:
    rows = []
    for fact in _json_list(facts_json):
        if not isinstance(fact, dict):
            continue
        rows.append(
            (
                str(fact.get("description") or ""),
                str(fact.get("source") or ""),
                str(fact.get("created") or ""),
                str(fact.get("updated") or ""),
            )
        )
    return rows


def _task_todos_panel(todos, drafts, conversation_titles: Mapping[str, str]) -> str:
    follow_ups_by_todo = _follow_up_drafts_by_todo(todos, drafts)
    items = "".join(
        _task_todo_detail_item(
            todo,
            follow_ups_by_todo.get(todo.id, []),
            conversation_titles,
        )
        for todo in todos
    )
    return f'<div class="todo-detail-list">{items}</div>'


def _follow_up_drafts_by_todo(todos, drafts) -> dict[int, list]:
    todo_ids = {todo.id for todo in todos}
    grouped = {todo.id: [] for todo in todos}
    for draft in drafts:
        if draft.todo_id in todo_ids:
            grouped[draft.todo_id].append(draft)
    return grouped


def _unlinked_follow_up_drafts(todos, drafts) -> list:
    todo_ids = {todo.id for todo in todos}
    return [draft for draft in drafts if draft.todo_id not in todo_ids]


def _task_todo_detail_item(todo, follow_ups, conversation_titles: Mapping[str, str]) -> str:
    owner = todo.owner_name or todo.owner_user_id or "-"
    status = str(todo.status)
    priority = str(todo.priority)
    deadline = _format_local_time(todo.deadline_at) or todo.deadline_at or "-"
    next_follow_up = (
        _format_local_time(todo.next_follow_up_at) or todo.next_follow_up_at or "-"
    )
    evidence = _task_json_compact(todo.completion_evidence_json, "{}") or "-"
    check_class = "todo-detail-check done" if _task_todo_done(todo) else "todo-detail-check"
    check_icon = _lucide_icon("check") if _task_todo_done(todo) else ""
    status_class = _task_status_class(status)
    follow_up_panel = (
        _task_follow_up_child_panel(todo.id, follow_ups, conversation_titles)
        if follow_ups
        else ""
    )
    return (
        f'<article class="todo-detail-item" id="todo-{todo.id}">'
        '<div class="todo-detail-main">'
        f'<span class="{check_class}">{check_icon}</span>'
        '<div class="todo-detail-body">'
        '<div class="todo-detail-title-row">'
        f'<h3 class="todo-detail-title">{escape(todo.title or "-")}</h3>'
        f'<span class="task-state {escape(status_class)}">{escape(status)}</span>'
        "</div>"
        '<div class="todo-detail-meta">'
        f"<span>#{todo.id}</span>"
        f"<span>{escape(owner)}</span>"
        f"<span>{escape(priority)}</span>"
        f"<span>DDL {escape(deadline)}</span>"
        f"<span>Next {escape(next_follow_up)}</span>"
        "</div>"
        '<div class="todo-detail-fields">'
        f"{_task_todo_detail_field('Question', todo.follow_up_question or '-')}"
        f"{_task_todo_detail_field('Blocker', todo.blocker or '-')}"
        f"{_task_todo_detail_field('Evidence', evidence)}"
        "</div>"
        "</div>"
        "</div>"
        f"{follow_up_panel}"
        "</article>"
    )


def _task_status_class(status: str) -> str:
    return status.strip().lower().replace("_", "-").replace(" ", "-") or "unknown"


def _task_todo_detail_field(label: str, value: str) -> str:
    return (
        '<div class="todo-detail-field">'
        f'<div class="todo-detail-label">{escape(label)}</div>'
        f'<div class="todo-detail-value">{escape(value)}</div>'
        "</div>"
    )


def _task_follow_up_child_panel(
    todo_id: int,
    drafts,
    conversation_titles: Mapping[str, str],
) -> str:
    items = "".join(
        _task_follow_up_child_item(draft, conversation_titles) for draft in drafts
    )
    label = f"Follow-ups ({len(drafts)})"
    return (
        f'<div class="todo-detail-followups" data-parent-todo="{todo_id}">'
        f"<div class=\"todo-followup-heading\">{escape(label)}</div>"
        f"<ul class=\"todo-followup-list\">{items}</ul>"
        "</div>"
    )


def _task_follow_up_child_item(draft, conversation_titles: Mapping[str, str]) -> str:
    scheduled = _format_local_time(draft.scheduled_at) or draft.scheduled_at or "-"
    owner = draft.owner_name or draft.owner_user_id or "-"
    target = _task_follow_up_target(draft, conversation_titles)
    return (
        "<li class=\"todo-followup-item\">"
        "<div class=\"todo-followup-bubble\">"
        "<div class=\"todo-followup-head\">"
        f"<span class=\"todo-followup-recipient\">{escape(owner)}</span>"
        f"<span class=\"todo-followup-status\">{escape(draft.status)}</span>"
        f"<span class=\"todo-followup-time\">{escape(scheduled)}</span>"
        "</div>"
        f"<div class=\"todo-followup-message\">{escape(draft.question_text)}</div>"
        f"<div class=\"todo-followup-target\">{escape(target)}</div>"
        "</div>"
        "</li>"
    )


def _task_follow_up_target(
    draft,
    conversation_titles: Mapping[str, str] | None = None,
) -> str:
    conversation_titles = conversation_titles or {}
    if draft.target_conversation_id and draft.target_conversation_id in conversation_titles:
        return conversation_titles[draft.target_conversation_id]
    return (
        f"{draft.target_kind}:{draft.target_conversation_id}"
        if draft.target_conversation_id
        else draft.target_kind or "-"
    )


def _task_update_rows(updates) -> list[tuple[str, str, str, str, str, str]]:
    rows = []
    for update in updates:
        source = f"{update.source_type}:{update.source_ref}".strip(":")
        rows.append(
            (
                _format_local_time(update.created_at),
                source,
                update.summary,
                _task_json_compact(update.changes_json, "{}"),
                update.merge_reason,
                f"{update.confidence:.2f}",
            )
        )
    return rows


def _task_follow_up_rows(
    drafts,
    conversation_titles: Mapping[str, str],
) -> list[tuple[str, str, str, str, str, str, str, str]]:
    rows = []
    for draft in drafts:
        target = _task_follow_up_target(draft, conversation_titles)
        todo_link = "-"
        if draft.todo_id:
            todo_link = f"<a href=\"#todo-{draft.todo_id}\">#{draft.todo_id}</a>"
        rows.append(
            (
                _format_local_time(draft.scheduled_at) or draft.scheduled_at,
                draft.owner_name or draft.owner_user_id,
                todo_link,
                target,
                str(draft.status),
                draft.question_text,
                _task_json_compact(draft.risk_check_json, "{}"),
                _task_json_compact(draft.send_result_json, "{}"),
            )
        )
    return rows


def _task_detail_cell(label: str, value: str) -> str:
    return (
        "<div class=\"attempt-detail-cell\">"
        f"<div class=\"attempt-detail-label\">{escape(label)}</div>"
        f"<div class=\"attempt-detail-value\">{escape(value)}</div>"
        "</div>"
    )


def _detail_page_header(
    title: str,
    subtitle: str = "",
    back_href: str = "/",
    back_label: str = "返回处理记录",
) -> str:
    subtitle_html = f"<p>{escape(subtitle)}</p>" if subtitle.strip() else ""
    return (
        "<div class=\"attempt-detail-page-head\">"
        f"<a class=\"attempt-detail-back\" href=\"{escape(back_href)}\">"
        f"{_lucide_icon('arrow-left')}<span>{escape(back_label)}</span></a>"
        "<div class=\"attempt-detail-heading\">"
        f"<h2>{escape(title)}</h2>"
        f"{subtitle_html}"
        "</div>"
        "</div>"
    )


def _simple_table(
    headers: Iterable[str],
    rows: Iterable[Iterable[str]],
    *,
    column_widths: Mapping[str, str] | None = None,
    html_columns: set[str] | None = None,
) -> str:
    header_values = tuple(headers)
    column_widths = column_widths or {}
    html_columns = html_columns or set()
    colgroup_html = "".join(
        f"<col style=\"width:{escape(column_widths.get(header, 'auto'))}\">"
        for header in header_values
    )
    header_html = "".join(f"<th>{escape(header)}</th>" for header in header_values)
    row_html = "".join(_simple_table_row(header_values, row, html_columns) for row in rows)
    table_class = ' class="column-sized-table"' if column_widths else ""
    colgroup = f"<colgroup>{colgroup_html}</colgroup>" if column_widths else ""
    return (
        f"<table{table_class}>"
        f"{colgroup}"
        "<thead><tr>"
        f"{header_html}"
        "</tr></thead><tbody>"
        f"{row_html}"
        "</tbody></table>"
    )


def _simple_table_row(
    header_values: tuple[str, ...],
    row: Iterable[str],
    html_columns: set[str],
) -> str:
    return (
        "<tr>"
        + "".join(
            f"<td>{value if header in html_columns else escape(value)}</td>"
            for header, value in zip(header_values, row)
        )
        + "</tr>"
    )


def _json_list(text: str) -> list:
    try:
        payload = json.loads(text or "[]")
    except json.JSONDecodeError:
        return []
    return payload if isinstance(payload, list) else []


def _task_json_compact(text: str, default: str) -> str:
    try:
        payload = json.loads(text or default)
    except json.JSONDecodeError:
        return text
    if payload in ({}, []):
        return ""
    return _excerpt(json.dumps(payload, ensure_ascii=False), 260)


def render_user_feedback_list(
    store: AutoReplyStore, limit: int = 50, page: int = 1
) -> str:
    total_count = store.count_user_feedback_items()
    page = _bounded_page(page, limit, total_count)
    offset = _page_offset(page, limit)
    rows = []
    for item in store.list_user_feedback_items(limit=limit, offset=offset):
        status = _user_feedback_status(item)
        attempt_link = (
            f"<a class=\"review-link\" href=\"/attempts/{item.attempt_id}\">处理</a>"
            if item.attempt_id
            else "<span class=\"muted\">未关联</span>"
        )
        resolve_action = _user_feedback_resolve_action(item, status)
        context_lines = [
            value
            for value in (
                item.conversation_title,
                item.trigger_sender,
                _excerpt(item.trigger_text, 140),
            )
            if value
        ]
        context_html = (
            f"<div class=\"user-feedback-context\">{escape(' · '.join(context_lines))}</div>"
            if context_lines
            else ""
        )
        comment = item.comment.strip() or "未填写评语"
        rows.append(
            "<tr>"
            f"<td><span class=\"pill status-{escape(status)}\">{escape(status)}</span></td>"
            f"<td>{escape(_feedback_rating_stars_for_rating(item.rating) or item.rating_label or item.rating)}</td>"
            "<td>"
            f"<div class=\"user-feedback-comment\">{escape(comment)}</div>"
            f"{context_html}"
            "</td>"
            f"<td>{escape(_format_local_time(item.received_at or item.updated_at))}</td>"
            f"<td><div class=\"user-feedback-actions\">{attempt_link}{resolve_action}</div></td>"
            "</tr>"
        )
    if rows:
        pagination = _pagination_controls(
            base_path="/user-feedback",
            page=page,
            limit=limit,
            total_count=total_count,
        )
        body = (
            "<section class=\"card\">"
            f"{_user_feedback_page_head()}"
            f"{pagination}"
            "<table class=\"user-feedback-table\"><thead><tr>"
            "<th>状态</th><th>评分</th><th>用户反馈</th><th>时间</th><th>操作</th>"
            "</tr></thead><tbody>"
            + "".join(rows)
            + "</tbody></table>"
            f"{_pagination_controls(base_path='/user-feedback', page=page, limit=limit, total_count=total_count, bottom=True)}"
            "</section>"
        )
    else:
        body = (
            "<section class=\"card\">"
            f"{_user_feedback_page_head()}"
            "<p class=\"muted\">暂无用户反馈。</p></section>"
        )
    return render_page(
        "用户反馈",
        body,
        active_nav="user-feedback",
        user_feedback_pending_count=store.count_pending_user_feedback_items(),
    )


def _user_feedback_page_head() -> str:
    return (
        "<div class=\"card-head\"><h2>用户反馈</h2>"
        "<form method=\"post\" action=\"/user-feedback/sync\">"
        "<button class=\"compact-button\" type=\"submit\">同步最新反馈</button>"
        "</form></div>"
    )


def _user_feedback_status(item: UserFeedbackItem) -> str:
    if (
        item.resolved_at.strip()
        or item.reviewer_feedback.strip()
        or item.corrected_reply_text.strip()
    ):
        return "resolved"
    return "pending"


def _user_feedback_resolve_action(item: UserFeedbackItem, status: str) -> str:
    if status == "resolved":
        return "<span class=\"muted\">已处理</span>"
    return (
        "<form method=\"post\" action=\"/user-feedback/resolve\">"
        f"<input type=\"hidden\" name=\"key\" value=\"{escape(item.key)}\">"
        "<button type=\"submit\">标记 resolved</button>"
        "</form>"
    )


def _reply_task_item(task: ReplyTask) -> str:
    error_html = (
        f"<div class=\"attempt-foot\"><span class=\"attempt-warning\">{escape(task.error)}</span></div>"
        if task.error and task.error != FAST_PATH_UNREAD_BACKOFF_TASK_ERROR
        else ""
    )
    return (
        "<article class=\"attempt-item\">"
        "<div class=\"attempt-head\">"
        "<div class=\"attempt-title\">"
        f"<span class=\"attempt-id\">#task-{task.id}</span>"
        f"{_status_action_pill(_display_action_state(task.status), task.status, 'message-circle')}"
        f"<div class=\"attempt-main\">{escape(task.conversation_title)}</div>"
        f"<div class=\"attempt-meta\">{escape(task.trigger_sender)}</div>"
        "</div>"
        "<div class=\"attempt-side\">"
        f"<time class=\"attempt-time\">{escape(_format_local_time(task.updated_at))}</time>"
        "</div>"
        "</div>"
        "<div class=\"attempt-lines\">"
        f"{_attempt_text_line('问', task.trigger_text, 260)}"
        f"{_attempt_text_line('进', _reply_task_progress_text(task), 320)}"
        "</div>"
        f"{error_html}"
        "</article>"
    )


def _reply_task_progress_text(task: ReplyTask) -> str:
    if task.status == "pending":
        if task.error == FAST_PATH_UNREAD_BACKOFF_TASK_ERROR:
            available_at = _format_local_time(task.available_at)
            return f"快路径已触发，等待到 {available_at} 后确认是否仍需处理"
        return "已进入处理队列，等待分身生成回复"
    if task.status == "processing":
        return "分身正在处理"
    if task.error:
        return task.error
    return "任务尚未完成"


def render_attempt_detail(store: AutoReplyStore, attempt_id: int) -> tuple[int, str]:
    attempt = store.get_reply_attempt(attempt_id)
    if attempt is None:
        return 404, render_page(
            "未找到处理记录",
            f"<p>处理记录 #{attempt_id} 不存在。</p>",
        )
    sent_reply = store.get_sent_reply(
        attempt.conversation_id,
        attempt.trigger_message_id,
    )
    feedback_events = _feedback_events_for_sent_reply(
        sent_reply,
        _feedback_events_by_sent_reply(store, [sent_reply] if sent_reply else []),
    )
    codex_session_id = attempt.codex_session_id or store.get_codex_session_id(
        attempt.conversation_id
    )
    return 200, render_page(
        f"事件详情 #{attempt.id}",
        _attempt_detail_body(attempt, sent_reply, codex_session_id, feedback_events),
        user_feedback_pending_count=store.count_pending_user_feedback_items(),
        show_nav=False,
        show_header=False,
    )


def render_codex_session_list(store: AutoReplyStore) -> str:
    rows = []
    for conversation in store.list_codex_conversations():
        session_id = conversation.codex_session_id or ""
        session_href = f"/codex/{quote(session_id, safe='')}" if session_id else ""
        latest_attempts = store.list_reply_attempts_for_conversation(
            conversation.conversation_id,
            limit=1,
        )
        history_cell = _attempt_link(latest_attempts[0]) if latest_attempts else ""
        row_attrs = (
            f" data-session-href=\"{escape(session_href)}\" tabindex=\"0\""
            if session_href
            else ""
        )
        rows.append(
            f"<tr{row_attrs}>"
            f"<td>{escape(conversation.title)}</td>"
            f"<td>{escape(conversation.conversation_id)}</td>"
            f"<td>{escape('私聊' if conversation.single_chat else '群聊')}</td>"
            f"<td><a href=\"{escape(session_href)}\">{escape(session_id)}</a></td>"
            f"<td>{history_cell}</td>"
            "</tr>"
        )
    table = (
        "<table class=\"codex-session-table\"><thead><tr><th>会话</th><th>会话 ID</th><th>类型</th>"
        "<th>执行会话</th><th>处理记录</th></tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )
    return render_page(
        "执行会话",
        table + _clickable_codex_session_rows_script(),
        active_nav="codex",
        user_feedback_pending_count=store.count_pending_user_feedback_items(),
    )


def render_codex_session_detail(
    session_id: str,
    codex_home: Path | None = None,
    store: AutoReplyStore | None = None,
) -> tuple[int, str]:
    rendered = render_local_codex_session(session_id, codex_home=codex_home)
    if rendered.missing:
        related_attempts = (
            store.list_reply_attempts_for_codex_session(session_id) if store else []
        )
        if related_attempts:
            body = (
                f"{_detail_page_header('执行记录不可用', session_id, '/codex', '返回执行会话')}"
                "<section class=\"card\"><h2>执行记录不可用</h2>"
                "<p class=\"muted\">这条执行会话对应的本地 transcript 文件已经不在这台机器上。</p>"
                f"<p class=\"muted\">{escape(session_id)}</p></section>"
                f"{_related_history_card(related_attempts, session_id=session_id, store=store)}"
            )
            return 200, render_page(
                "执行记录不可用",
                body,
                user_feedback_pending_count=(
                    store.count_pending_user_feedback_items() if store else None
                ),
                show_nav=False,
                show_header=False,
            )
        return 404, render_page(
            "未找到执行会话",
            f"{_detail_page_header('未找到执行会话', session_id, '/codex', '返回执行会话')}"
            f"<section class=\"card\"><p>未找到执行会话：{escape(session_id)}</p></section>",
            user_feedback_pending_count=(
                store.count_pending_user_feedback_items() if store else None
            ),
            show_nav=False,
            show_header=False,
        )
    events = "".join(_codex_event_card(event) for event in rendered.events)
    related_history = _related_history_card(
        store.list_reply_attempts_for_codex_session(session_id) if store else [],
        session_id=session_id,
        store=store,
    )
    body = (
        f"{_detail_page_header(f'执行会话 {session_id}', back_href='/codex', back_label='返回执行会话')}"
        "<section class=\"card\"><div class=\"grid\">"
        f"<div class=\"muted\">会话 ID</div><div>{escape(rendered.session_id)}</div>"
        f"<div class=\"muted\">本地文件</div><div>{escape(str(rendered.path or ''))}</div>"
        f"<div class=\"muted\">事件数</div><div>{len(rendered.events)}</div>"
        "</div></section>"
        f"{related_history}"
        f"{events}"
    )
    return 200, render_page(
        f"执行会话 {session_id}",
        body,
        user_feedback_pending_count=(
            store.count_pending_user_feedback_items() if store else None
        ),
        show_nav=False,
        show_header=False,
    )


def render_error_list(
    store: AutoReplyStore,
    limit: int | None = DEFAULT_ERROR_LIST_LIMIT,
    page: int = 1,
) -> str:
    return render_log_list(store, limit=limit, page=page)


def render_log_list(
    store: AutoReplyStore,
    limit: int | None = DEFAULT_ERROR_LIST_LIMIT,
    page: int = 1,
    query: str = "",
    log_type: str = "",
) -> str:
    query = query.strip()
    log_type = log_type.strip()
    total_count = store.count_operation_logs(query=query, log_type=log_type)
    page = _bounded_page(page, limit, total_count)
    offset = _page_offset(page, limit)
    items = _log_page_items(
        store,
        limit=limit,
        offset=offset,
        query=query,
        log_type=log_type,
    )
    page_count = _page_count(total_count, limit)
    has_more = page < page_count
    next_page = page + 1 if has_more else ""
    toolbar = _log_toolbar(
        query=query,
        log_type=log_type,
        log_types=store.list_operation_log_types(),
        page=page,
        limit=limit,
        total_count=total_count,
    )
    body = (
        f"{toolbar}"
        f"<div data-live-search-region=\"logs\" data-infinite-list=\"logs\" "
        f"data-next-page=\"{escape(str(next_page))}\" data-has-more=\"{'1' if has_more else '0'}\">"
        f"<section class=\"log-feed\" data-infinite-items>{''.join(items)}</section>"
        f"{_infinite_load_status(has_more=has_more)}"
        "</div>"
        f"{_infinite_list_script()}"
    )
    return render_page(
        "运行日志",
        body,
        active_nav="logs",
        user_feedback_pending_count=store.count_pending_user_feedback_items(),
    )


def _log_toolbar(
    *,
    query: str,
    log_type: str,
    log_types: list[str],
    page: int,
    limit: int | None,
    total_count: int,
) -> str:
    return _table_toolbar(
        name="logs",
        action="/logs",
        search_label="搜索运行日志",
        search_name="q",
        query=query,
        type_select_html=_log_type_select(log_type=log_type, log_types=log_types),
        page_links_html="",
        page_size_select_html="",
        total_count=total_count,
    )


def _log_page_href(*, page: int, limit: int | None, query: str, log_type: str) -> str:
    params: dict[str, str] = {}
    if page > 1:
        params["page"] = str(page)
    if limit is not None and limit != DEFAULT_ERROR_LIST_LIMIT:
        params["limit"] = str(limit)
    if query:
        params["q"] = query
    if log_type:
        params["type"] = log_type
    if not params:
        return "/logs"
    return f"/logs?{urlencode(params)}"


def _log_type_select(*, log_type: str, log_types: list[str]) -> str:
    options = [f"<option value=\"\"{' selected' if not log_type else ''}>全部类型</option>"]
    options.extend(
        f"<option value=\"{escape(value)}\"{' selected' if value == log_type else ''}>"
        f"{escape(_log_type_display(value))}</option>"
        for value in log_types
    )
    return (
        "<select name=\"type\" class=\"table-type-select\" "
        "aria-label=\"日志类型筛选\" onchange=\"this.form.submit()\">"
        f"{''.join(options)}</select>"
    )


def _log_type_display(value: str) -> str:
    return {
        "Error": "错误",
        "Reply": "回复",
        "Reply task": "回复任务",
        "Task input": "任务输入",
        "Task update": "任务更新",
    }.get(value, value)


def _log_page_size_select(*, limit: int | None) -> str:
    selected_limit = limit or DEFAULT_ERROR_LIST_LIMIT
    options_values = sorted({*LOG_PAGE_SIZE_OPTIONS, selected_limit})
    options = "".join(
        f"<option value=\"{size}\"{' selected' if size == selected_limit else ''}>{size}/页</option>"
        for size in options_values
    )
    return (
        "<select name=\"limit\" class=\"table-page-size\" "
        "aria-label=\"每页日志数\" onchange=\"this.form.submit()\">"
        f"{options}</select>"
    )


def _log_page_items(
    store: AutoReplyStore,
    *,
    limit: int | None,
    offset: int,
    query: str,
    log_type: str,
) -> list[str]:
    items = []
    for log in store.list_operation_logs(
        limit=limit,
        offset=offset,
        query=query,
        log_type=log_type,
    ):
        status = _operation_log_status(store, log)
        status_class = _operation_status_class(status)
        items.append(_operation_log_item(log, status, status_class))
    return items


def render_log_page_chunk(
    store: AutoReplyStore,
    *,
    limit: int | None = DEFAULT_ERROR_LIST_LIMIT,
    page: int = 1,
    query: str = "",
    log_type: str = "",
) -> dict[str, object]:
    query = query.strip()
    log_type = log_type.strip()
    total_count = store.count_operation_logs(query=query, log_type=log_type)
    page = _bounded_page(page, limit, total_count)
    items = _log_page_items(
        store,
        limit=limit,
        offset=_page_offset(page, limit),
        query=query,
        log_type=log_type,
    )
    page_count = _page_count(total_count, limit)
    has_more = page < page_count
    return {
        "items_html": "".join(items),
        "page": page,
        "next_page": page + 1 if has_more else None,
        "has_more": has_more,
        "total_count": total_count,
    }


def _operation_log_item(log: OperationLog, status: str, status_class: str) -> str:
    summary = _excerpt(log.summary, 420) if log.summary else ""
    detail = _excerpt(log.detail, 420) if log.detail else ""
    if not detail or detail == summary:
        body = (
            "<div class=\"log-body single\">"
            f"{_operation_log_field('Summary', summary or '-')}"
            "</div>"
        )
    else:
        body = (
            "<div class=\"log-body\">"
            f"{_operation_log_field('Summary', summary or '-')}"
            f"{_operation_log_field('Detail', detail)}"
            "</div>"
        )
    return (
        "<article class=\"log-item\">"
        "<div class=\"log-main\">"
        "<div class=\"log-head\">"
        "<div class=\"log-title\">"
        f"<span class=\"pill\">{escape(log.category)}</span>"
        f"<span class=\"log-action\">{escape(log.action or '-')}</span>"
        f"<span class=\"pill {status_class}\">{escape(status or '-')}</span>"
        "</div>"
        f"<time class=\"log-time\">{escape(_format_local_time(log.occurred_at))}</time>"
        "</div>"
        "<div class=\"log-meta\">"
        f"<span>{escape(log.id)}</span>"
        f"<span class=\"log-context\">{escape(log.context or '-')}</span>"
        "</div>"
        f"{body}"
        "</div>"
        "</article>"
    )


def _operation_log_field(label: str, value: str) -> str:
    return (
        "<div class=\"log-field\">"
        f"<div class=\"log-label\">{escape(label)}</div>"
        f"<div class=\"log-value\">{escape(value)}</div>"
        "</div>"
    )


def _operation_log_status(store: AutoReplyStore, log: OperationLog) -> str:
    if log.source_table != "errors":
        return log.status
    error = ReplyError(
        id=log.source_id,
        conversation_id=log.conversation_id or None,
        message_id=log.message_id or None,
        kind=log.action,
        detail=log.detail,
        created_at=log.occurred_at,
    )
    return _error_resolution_label(store, error)


def _operation_status_class(status: str) -> str:
    normalized = status.strip().lower()
    if normalized.startswith("resolved") or normalized in {"sent", "done", "completed"}:
        return "status-resolved"
    if normalized in {"failed", "blocked"}:
        return "status-failed"
    return "status-active"


def render_developer_prompt_editor(
    *,
    active_tab: str = "developer",
    saved: bool = False,
) -> str:
    if active_tab not in {"developer", "user"}:
        active_tab = "developer"
    return render_config_page(active_tab=active_tab, saved=saved)


def _render_developer_prompt_editor_content(*, saved: bool = False) -> str:
    template_path = developer_prompt_template_path()
    error_html = ""
    try:
        template = read_developer_prompt_template()
    except OSError as exc:
        template = ""
        error_html = (
            "<p class=\"attempt-warning\">"
            f"无法读取模板：{escape(str(exc))}"
            "</p>"
        )
    _, body_template = split_developer_prompt_template(template)
    try:
        preview = render_developer_prompt_template(template) if template else ""
    except DeveloperPromptTemplateError as exc:
        preview = ""
        error_html = (
            "<p class=\"attempt-warning\">"
            f"模板渲染失败：{escape(str(exc))}"
            "</p>"
        )
    saved_html = "<p class=\"muted\">已保存。</p>" if saved else ""
    return (
        "<section class=\"card\">"
        "<div class=\"grid\">"
        "<div class=\"muted\">模板路径</div>"
        f"<div>{escape(str(template_path))}</div>"
        "</div>"
        f"{saved_html}{error_html}"
        "<form method=\"post\" action=\"/config?tab=developer\">"
        "<label for=\"template\">模板内容</label>"
        f"<textarea id=\"template\" name=\"template\" style=\"min-height:520px\">{escape(body_template)}</textarea>"
        "<p><button type=\"submit\">保存模板</button></p>"
        "</form>"
        "</section>"
        "<section class=\"card\">"
        "<h2>渲染预览</h2>"
        f"<pre>{escape(preview)}</pre>"
        "</section>"
    )


def _render_user_prompt_editor_content(*, saved: bool = False) -> str:
    template_path = user_prompt_template_path()
    error_html = ""
    try:
        template = read_user_prompt_template()
    except OSError as exc:
        template = ""
        error_html = (
            "<p class=\"attempt-warning\">"
            f"无法读取模板：{escape(str(exc))}"
            "</p>"
        )
    try:
        preview = render_user_prompt_template(template, {}) if template else ""
    except DeveloperPromptTemplateError as exc:
        preview = ""
        error_html = (
            "<p class=\"attempt-warning\">"
            f"模板渲染失败：{escape(str(exc))}"
            "</p>"
        )
    saved_html = "<p class=\"muted\">已保存。</p>" if saved else ""
    return (
        "<section class=\"card\">"
        "<div class=\"grid\">"
        "<div class=\"muted\">模板路径</div>"
        f"<div>{escape(str(template_path))}</div>"
        "</div>"
        f"{saved_html}{error_html}"
        "<form method=\"post\" action=\"/config?tab=user\">"
        "<label for=\"template\">模板内容</label>"
        f"<textarea id=\"template\" name=\"template\" style=\"min-height:520px\">{escape(template)}</textarea>"
        "<p><button type=\"submit\">保存模板</button></p>"
        "</form>"
        "</section>"
        "<section class=\"card\">"
        "<h2>渲染预览</h2>"
        f"<pre>{escape(preview)}</pre>"
        "</section>"
    )


def _user_prompt_dynamic_function_table() -> str:
    blocks = [
        UserPromptBlock(
            name="work_profile_instruction",
            expression="app.prompt:work_profile_instruction()",
            description="读取并注入工作人格 Profile；通常用于 Developer Prompt。",
            default=(
                "工作人格 Profile:\n"
                "- 由服务端注入；不要再尝试读取 profile 文件路径。\n"
                "- 用于学习判断顺序、追问方式和回复边界。"
            ),
        ),
        *USER_PROMPT_BLOCKS,
    ]
    rows = [
        "<tr><th>函数</th><th>说明</th><th>默认预览</th></tr>",
        *[
            "<tr>"
            f"<td><code>{escape(block.name)}()</code><br>"
            f"<code>&lt;code: {escape(block.expression)}&gt;</code></td>"
            f"<td>{escape(block.description)}</td>"
            f"<td><pre class=\"dynamic-preview\">{escape(block.default)}</pre></td>"
            "</tr>"
            for block in blocks
        ],
    ]
    return "<table>" + "".join(rows) + "</table>"


def _error_resolution_label(store: AutoReplyStore, error: ReplyError) -> str:
    if not error.conversation_id or not error.message_id:
        return "待处理"
    if store.get_sent_reply(error.conversation_id, error.message_id):
        return "已解决：已发送"
    attempt = store.get_latest_reply_attempt_for_trigger(
        error.conversation_id,
        error.message_id,
    )
    if attempt and attempt.send_status == "sent":
        return "已解决：已发送"
    return "待处理"


def handle_feedback_post(
    store: AutoReplyStore, attempt_id: int, body: bytes
) -> tuple[int, dict[str, str], str]:
    parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True)
    feedback = parsed.get("feedback", [""])[0]
    corrected_reply = parsed.get("corrected_reply", [""])[0]
    if not store.record_reply_feedback(
        attempt_id,
        feedback=feedback,
        corrected_reply_text=corrected_reply,
    ):
        return 404, {}, render_page("未找到处理记录", "未找到处理记录")
    return 303, {"Location": f"/attempts/{attempt_id}"}, ""


def handle_user_feedback_resolve_post(
    store: AutoReplyStore, body: bytes
) -> tuple[int, dict[str, str], str]:
    parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True)
    key = parsed.get("key", [""])[0]
    if not store.resolve_feedback_event(key):
        return 404, {}, render_page("未找到反馈", "未找到反馈")
    return 303, {"Location": "/user-feedback"}, ""


def handle_user_feedback_sync_post(
    store: AutoReplyStore,
) -> tuple[int, dict[str, str], str]:
    _sync_feedback_events_for_sent_replies(
        store,
        store.list_sent_replies_waiting_for_feedback_events(),
    )
    return 303, {"Location": "/user-feedback"}, ""


def handle_developer_prompt_post(body: bytes) -> tuple[int, dict[str, str], str]:
    parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True)
    template = parsed.get("template", [""])[0]
    write_developer_prompt_template(template.strip())
    return 303, {"Location": "/config?tab=recipe&saved=1"}, ""


def handle_prompt_variables_post(body: bytes) -> tuple[int, dict[str, str], str]:
    parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True)
    active_tab = parsed.get("active_tab", ["info"])[0]
    tab_aliases = {
        "info": "recipe",
        "developer": "recipe",
        "user": "recipe",
        "dingtalk": "runtime",
        "reply": "runtime",
    }
    active_tab = tab_aliases.get(active_tab, active_tab)
    if active_tab not in {"recipe", "memory", "runtime", "system"}:
        active_tab = "recipe"
    write_configurable_prompt_variables(
        [
            (key, value)
            for key, value in zip_longest(
                parsed.get("variable_key", []),
                parsed.get("variable_value", []),
                fillvalue="",
            )
        ]
    )
    return 303, {"Location": f"/config?tab={active_tab}&saved=1"}, ""


def handle_system_config_post(body: bytes) -> tuple[int, dict[str, str], str]:
    parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True)
    editable_keys = _editable_system_config_keys()
    updates = {
        key: value
        for key, value in zip_longest(
            parsed.get("system_key", []),
            parsed.get("system_value", []),
            fillvalue="",
        )
        if key in editable_keys
    }
    write_env_values(updates)
    return 303, {"Location": "/config?tab=runtime&saved=1"}, ""


def handle_dingtalk_reconnect_post(dws: DwsClient | None = None) -> tuple[int, dict[str, str], str]:
    client = dws or DwsClient()
    try:
        try:
            client.start_auth_login(force=True)
        except TypeError:
            client.start_auth_login()
    except Exception:
        return 303, {"Location": "/config?tab=runtime&dingtalk=reconnect-failed"}, ""
    return 303, {"Location": "/config?tab=runtime&dingtalk=reconnect-started"}, ""


def handle_user_prompt_post(body: bytes) -> tuple[int, dict[str, str], str]:
    parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True)
    template = parsed.get("template", [""])[0]
    write_user_prompt_template(template)
    return 303, {"Location": "/config?tab=recipe&saved=1"}, ""


def handle_recall_post(
    store: AutoReplyStore, dws, attempt_id: int, *, return_to: str = ""
) -> tuple[int, dict[str, str], str]:
    attempt = store.get_reply_attempt(attempt_id)
    if attempt is None:
        return 404, {}, render_page("未找到处理记录", "未找到处理记录")
    sent_reply = store.get_sent_reply(
        attempt.conversation_id,
        attempt.trigger_message_id,
    )
    if sent_reply is None:
        return (
            400,
            {},
            render_page(
                "撤销不可用",
                "<p>撤销不可用：没有找到这条处理记录对应的已发送回复。</p>",
            ),
        )
    message_id = _sent_reply_recall_message_id(sent_reply)
    if not message_id:
        open_task_id = _sent_reply_open_task_id(sent_reply)
        if open_task_id and hasattr(dws, "query_message_send_status"):
            try:
                message_id = _find_string_value(
                    dws.query_message_send_status(open_task_id),
                    _DWS_MESSAGE_ID_KEYS,
                )
            except Exception as exc:
                store.update_sent_reply_recall(
                    sent_reply.id,
                    recall_status="failed",
                    recall_error=str(exc),
                )
                return (
                    500,
                    {},
                    render_page("撤销失败", f"<p>{escape(str(exc))}</p>"),
                )
    if not message_id and not sent_reply.recall_key:
        return (
            400,
            {},
            render_page(
                "撤销不可用",
                "<p>撤销不可用：没有可撤销消息 ID 或 key，当前发送方式不支持自动撤销。</p>",
            ),
        )
    try:
        if message_id:
            dws.recall_message(attempt.conversation_id, message_id)
        else:
            dws.recall_bot_message(attempt.conversation_id, sent_reply.recall_key)
    except Exception as exc:
        store.update_sent_reply_recall(
            sent_reply.id,
            recall_status="failed",
            recall_error=str(exc),
        )
        return (
            500,
            {},
            render_page("撤销失败", f"<p>{escape(str(exc))}</p>"),
        )
    store.update_sent_reply_recall(
        sent_reply.id,
        recall_status="recalled",
        recall_error="",
    )
    return 303, {"Location": _safe_action_return_to(return_to, attempt_id)}, ""


def _audit_worker_settings(db_path: Path):
    from app.cli import DEFAULT_DING_ROBOT_NAME, WorkerSettings

    return WorkerSettings(
        workspace=workspace_path(),
        db_path=db_path,
        corpus_dir=corpus_dir(),
        dry_run=False,
        ding_robot_code=os.getenv("CEO_DING_ROBOT_CODE")
        or os.getenv("DINGTALK_DING_ROBOT_CODE"),
        ding_robot_name=os.getenv("CEO_DING_ROBOT_NAME", DEFAULT_DING_ROBOT_NAME),
        ding_receiver_user_id=os.getenv("CEO_DING_RECEIVER_USER_ID"),
    )


def _create_audit_worker(settings):
    from app.cli import create_worker

    return create_worker(settings)


def handle_rerun_attempt_post(
    store: AutoReplyStore,
    attempt_id: int,
    *,
    return_to: str = "",
    worker_factory: Callable[[object], object] | None = None,
) -> tuple[int, dict[str, str], str]:
    attempt = store.get_reply_attempt(attempt_id)
    if attempt is None:
        return 404, {}, render_page("未找到处理记录", "未找到处理记录")
    conversation_record = store.get_conversation(attempt.conversation_id)
    if conversation_record is None:
        return (
            404,
            {},
            render_page(
                "未找到会话",
                f"<p>未找到会话：{escape(attempt.conversation_id)}</p>",
            ),
        )
    settings = _audit_worker_settings(store.path)
    worker = (worker_factory or _create_audit_worker)(settings)
    conversation = DingTalkConversation(
        open_conversation_id=conversation_record.conversation_id,
        title=conversation_record.title,
        single_chat=conversation_record.single_chat,
        unread_point=1,
    )
    try:
        processed_message_id = worker.rerun_message(
            conversation,
            attempt.trigger_message_id,
            force_new_decision=True,
            oa_url=attempt.oa_url,
        )
    except (SystemExit, ValueError) as exc:
        return 400, {}, render_page("重跑失败", f"<p>{escape(str(exc))}</p>")
    store.complete_reply_task_for_message(
        attempt.conversation_id,
        processed_message_id,
    )
    return 303, {"Location": _safe_action_return_to(return_to, attempt_id)}, ""


def _safe_action_return_to(return_to: str, attempt_id: int) -> str:
    cleaned = return_to.strip()
    if cleaned.startswith("/codex/") or cleaned == f"/attempts/{attempt_id}":
        return cleaned
    return f"/attempts/{attempt_id}"


def handle_reviewed_message_reply(
    store: AutoReplyStore,
    dws: DwsClient,
    *,
    user_name: str,
    group_name: str,
    message_str: str,
    reply_text: str,
    reviewer_feedback: str = "",
) -> dict[str, object]:
    conversations = dws.search_conversations(group_name)
    exact_conversations = [
        conversation for conversation in conversations if conversation.title == group_name
    ]
    stored_conversation = None
    if len(exact_conversations) != 1:
        stored_conversation = store.find_conversation_by_title(group_name)
    if len(exact_conversations) != 1 and stored_conversation is not None:
        exact_conversations = [
            DingTalkConversation(
                open_conversation_id=stored_conversation.conversation_id,
                title=stored_conversation.title,
                single_chat=stored_conversation.single_chat,
                unread_point=1,
            )
        ]
    if len(exact_conversations) != 1:
        raise ValueError(
            f"expected one conversation named {group_name!r}, got {len(exact_conversations)}"
        )
    conversation = exact_conversations[0]
    messages = _reviewed_reply_lookup_messages(dws, conversation)
    matches = [
        message
        for message in messages
        if message.sender_name == user_name and message.content == message_str
    ]
    if not matches:
        raise ValueError("message not found for user_name/group_name/message_str")
    trigger = matches[0]
    store.upsert_conversation(
        conversation_id=conversation.open_conversation_id,
        title=conversation.title,
        single_chat=conversation.single_chat,
        codex_session_id=None,
    )
    attempt_id = store.record_reply_attempt(
        conversation_id=conversation.open_conversation_id,
        conversation_title=conversation.title,
        trigger_message_id=trigger.open_message_id,
        trigger_sender=trigger.sender_name,
        trigger_text=trigger.content,
        action=CodexAction.SEND_REPLY.value,
        sensitivity_kind=SensitivityKind.GENERAL.value,
        codex_reason="reviewed_message_reply",
        draft_reply_text=reply_text,
        audit_tool_events_json=json.dumps(
            [
                {
                    "tool": "audit_web.handle_reviewed_message_reply",
                    "result": "matched user_name/group_name/message_str",
                }
            ],
            ensure_ascii=False,
        ),
        audit_summary="已按发送人、群名、消息原文定位并处理。",
    )
    worker = DingTalkAutoReplyWorker(store=store, dws=dws, codex=None, dry_run=False)
    worker._send_reply(
        conversation=conversation,
        trigger=trigger,
        new_messages=[trigger],
        reply_text=reply_text,
        reason="reviewed_message_reply",
        attempt_id=attempt_id,
    )
    if reviewer_feedback.strip():
        store.record_reply_feedback(
            attempt_id,
            feedback=reviewer_feedback,
            corrected_reply_text=reply_text,
        )
    attempt = store.get_reply_attempt(attempt_id)
    if attempt is None:
        raise ValueError(f"reply attempt disappeared: {attempt_id}")
    return {
        "attempt_id": attempt_id,
        "conversation_title": conversation.title,
        "trigger_sender": trigger.sender_name,
        "trigger_text": trigger.content,
        "send_status": attempt.send_status,
        "final_reply_text": attempt.final_reply_text,
        "reviewer_feedback": attempt.reviewer_feedback,
    }


def _reviewed_reply_lookup_messages(
    dws: DwsClient,
    conversation: DingTalkConversation,
) -> list[DingTalkMessage]:
    seen_message_ids: set[str] = set()
    result: list[DingTalkMessage] = []
    lookup_batches = []
    if not conversation.single_chat:
        lookup_batches.append(dws.read_mentioned_messages(conversation, limit=100))
    lookup_batches.extend(
        [
            dws.read_recent_messages(conversation),
            dws.read_unread_messages(conversation),
        ]
    )
    for message in [message for batch in lookup_batches for message in batch]:
        if message.open_message_id in seen_message_ids:
            continue
        seen_message_ids.add(message.open_message_id)
        result.append(message)
    return result


def create_audit_app(
    db_path: Path,
    ding_robot_code: str | None = None,
    ding_robot_name: str | None = None,
) -> FastAPI:
    app = FastAPI(title="一人 CEO 工作台")

    @app.get("/", response_class=HTMLResponse)
    def attempt_list(request: Request) -> str:
        return render_attempt_list(
            AutoReplyStore(db_path),
            limit=DEFAULT_ATTEMPT_LIST_LIMIT,
            page=_positive_int_query(request, "page", default=1),
            type_filter=request.query_params.getlist("type"),
            query=str(request.query_params.get("q", "")),
        )

    @app.get("/api/history/updates")
    def history_updates(request: Request) -> JSONResponse:
        return JSONResponse(
            render_history_updates(
                AutoReplyStore(db_path),
                cursor=str(request.query_params.get("cursor", "")),
                limit=DEFAULT_ATTEMPT_LIST_LIMIT,
                page=_positive_int_query(request, "page", default=1),
                type_filter=request.query_params.getlist("type"),
                query=str(request.query_params.get("q", "")),
            )
        )

    @app.get("/api/history/page")
    def history_page_chunk(request: Request) -> JSONResponse:
        return JSONResponse(
            render_history_page_chunk(
                AutoReplyStore(db_path),
                limit=DEFAULT_ATTEMPT_LIST_LIMIT,
                page=_positive_int_query(request, "page", default=1),
                type_filter=request.query_params.getlist("type"),
                query=str(request.query_params.get("q", "")),
            )
        )

    @app.get("/user-feedback", response_class=HTMLResponse)
    def user_feedback_list(request: Request) -> str:
        return render_user_feedback_list(
            AutoReplyStore(db_path),
            page=_positive_int_query(request, "page", default=1),
        )

    @app.get("/tutorial", response_class=HTMLResponse)
    def tutorial_page() -> RedirectResponse:
        AutoReplyStore(db_path)
        return RedirectResponse("/", status_code=303)

    @app.get("/tutorial/status")
    def tutorial_status() -> JSONResponse:
        return JSONResponse(build_wizard_status(AutoReplyStore(db_path)).model_dump())

    @app.post("/tutorial/check/{step_id}", response_model=None)
    def tutorial_check(step_id: str, request: Request) -> Response:
        store = AutoReplyStore(db_path)
        try:
            step = get_step_definition(step_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Unknown setup step") from exc
        _require_available_setup_action(store, f"check_{step.id}", kind="check")
        status = check_setup_step(step_id, repo_root=_repo_root(), store=store)
        store.upsert_setup_wizard_step(
            step_id=status.step_id,
            status=status.status,
            summary=status.summary,
        )
        return _setup_action_response(request, status)

    @app.post("/tutorial/run/{action_id}", response_model=None)
    def tutorial_run(action_id: str, request: Request) -> Response:
        store = AutoReplyStore(db_path)
        _require_available_setup_action(store, action_id, kind="run")
        event = run_setup_action(action_id, repo_root=_repo_root(), env=dict(os.environ))
        store.record_setup_wizard_event(
            step_id=event.step_id,
            action_id=event.action_id,
            status=event.status,
            summary=event.summary,
            evidence_json=json.dumps(event.evidence, ensure_ascii=False),
            stdout_excerpt=event.stdout_excerpt,
            stderr_excerpt=event.stderr_excerpt,
        )
        if event.step_id != "unknown":
            store.upsert_setup_wizard_step(
                step_id=event.step_id,
                status="done" if event.status == "done" else "failed",
                summary=event.summary,
            )
        return _setup_action_response(request, event)

    @app.post("/tutorial/confirm/{step_id}", response_model=None)
    async def tutorial_confirm(
        step_id: str,
        request: Request,
    ) -> Response:
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            payload = await request.json()
        else:
            form_values = parse_qs(
                (await request.body()).decode(),
                keep_blank_values=True,
            )
            payload = {key: values[-1] for key, values in form_values.items()}
        evidence = payload.get("evidence")
        evidence_payload = evidence if isinstance(evidence, Mapping) else {}
        store = AutoReplyStore(db_path)
        _require_available_setup_action(store, f"confirm_{step_id}", kind="confirm")
        event = confirm_setup_step(
            step_id,
            store=store,
            confirmed_by=str(payload.get("confirmed_by") or "local-user"),
            evidence={
                key: str(value)
                for key, value in evidence_payload.items()
            },
        )
        store.record_setup_wizard_event(
            step_id=event.step_id,
            action_id=event.action_id,
            status=event.status,
            summary=event.summary,
            evidence_json=json.dumps(event.evidence, ensure_ascii=False),
            stdout_excerpt=event.stdout_excerpt,
            stderr_excerpt=event.stderr_excerpt,
        )
        return _setup_action_response(request, event)

    @app.get("/tasks", response_class=HTMLResponse)
    def tasks_page(request: Request) -> str:
        return render_tasks_page(
            AutoReplyStore(db_path),
            query=str(request.query_params.get("q") or ""),
            category=str(request.query_params.get("category") or ""),
            task_state=str(request.query_params.get("task_state") or ""),
            sort=str(request.query_params.get("sort") or ""),
            page=_positive_int_query(request, "page", default=1),
            page_size=DEFAULT_TASK_PAGE_SIZE,
        )

    @app.get("/api/tasks/page")
    def tasks_page_chunk(request: Request) -> JSONResponse:
        return JSONResponse(
            render_task_page_chunk(
                AutoReplyStore(db_path),
                query=str(request.query_params.get("q") or ""),
                category=str(request.query_params.get("category") or ""),
                task_state=str(request.query_params.get("task_state") or ""),
                sort=str(request.query_params.get("sort") or ""),
                page=_positive_int_query(request, "page", default=1),
                page_size=DEFAULT_TASK_PAGE_SIZE,
            )
        )

    @app.get("/tasks/{project_id}", response_class=HTMLResponse)
    def task_project_detail(project_id: int) -> HTMLResponse:
        status, html = render_task_project_detail(AutoReplyStore(db_path), project_id)
        return HTMLResponse(html, status_code=status)

    @app.get("/logs", response_class=HTMLResponse)
    def log_list(request: Request) -> str:
        return render_log_list(
            AutoReplyStore(db_path),
            limit=DEFAULT_ERROR_LIST_LIMIT,
            page=_positive_int_query(request, "page", default=1),
            query=str(request.query_params.get("q", "")),
            log_type=str(request.query_params.get("type", "")),
        )

    @app.get("/api/logs/page")
    def log_page_chunk(request: Request) -> JSONResponse:
        return JSONResponse(
            render_log_page_chunk(
                AutoReplyStore(db_path),
                limit=DEFAULT_ERROR_LIST_LIMIT,
                page=_positive_int_query(request, "page", default=1),
                query=str(request.query_params.get("q", "")),
                log_type=str(request.query_params.get("type", "")),
            )
        )

    @app.get("/errors", response_class=HTMLResponse)
    def error_list(request: Request) -> str:
        return log_list(request)

    @app.get("/codex", response_class=HTMLResponse)
    def codex_session_list() -> str:
        return render_codex_session_list(AutoReplyStore(db_path))

    @app.get("/codex/{session_id}", response_class=HTMLResponse)
    def codex_session_detail(session_id: str) -> HTMLResponse:
        status, html = render_codex_session_detail(
            session_id,
            store=AutoReplyStore(db_path),
        )
        return HTMLResponse(html, status_code=status)

    @app.get("/developer-prompt", response_class=HTMLResponse)
    def developer_prompt_editor(request: Request) -> str:
        tab = request.query_params.get("tab", "developer")
        saved_suffix = "&saved=1" if request.query_params.get("saved") == "1" else ""
        return RedirectResponse(f"/config?tab={tab}{saved_suffix}", status_code=303)

    @app.get("/config", response_class=HTMLResponse)
    def config_page(request: Request) -> str:
        return render_config_page(
            active_tab=request.query_params.get("tab", "info"),
            saved=request.query_params.get("saved") == "1",
            dingtalk_reconnect=str(request.query_params.get("dingtalk", "")),
            db_path=db_path,
        )

    @app.get("/notifications", response_class=HTMLResponse)
    def browser_notifications() -> str:
        return render_browser_notifications_page()

    @app.get("/notification-service-worker.js")
    def notification_service_worker() -> Response:
        return Response(
            _notification_service_worker_script(),
            media_type="application/javascript",
            headers={"Cache-Control": "no-cache"},
        )

    @app.get("/notifications/events")
    def browser_notification_events() -> StreamingResponse:
        return _browser_notification_event_stream()

    @app.post("/browser-notifications")
    async def browser_notification_post(request: Request) -> JSONResponse:
        payload = await request.json()
        event = _browser_notification_event(
            title=str(payload.get("title") or "CEO Agent"),
            message=str(payload.get("message") or ""),
            url=str(payload.get("url") or ""),
        )
        delivered = _publish_browser_notification(event)
        return JSONResponse(
            {
                "ok": True,
                "delivered": delivered,
                "subscribers": len(_BROWSER_NOTIFICATION_SUBSCRIBERS),
                "dingtalk_url": _dingtalk_url_from_bridge_url(event["url"]),
            }
        )

    @app.get("/dingtalk/open-chat-bridge", response_class=HTMLResponse)
    def dingtalk_open_chat_bridge(conversation_id: str) -> HTMLResponse:
        cleaned_conversation_id = conversation_id.strip()
        if not cleaned_conversation_id:
            return HTMLResponse("missing conversation_id", status_code=400)
        return HTMLResponse(render_dingtalk_open_chat_bridge(cleaned_conversation_id))

    @app.post("/dingtalk/bridge-status")
    async def dingtalk_bridge_status(request: Request) -> JSONResponse:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        _DINGTALK_BRIDGE_STATUS.append(
            {
                "conversation_id": str(payload.get("conversation_id") or ""),
                "stage": str(payload.get("stage") or ""),
                "detail": str(payload.get("detail") or ""),
            }
        )
        return JSONResponse({"ok": True})

    @app.get("/dingtalk/bridge-status")
    def dingtalk_bridge_status_list() -> JSONResponse:
        return JSONResponse({"events": list(_DINGTALK_BRIDGE_STATUS)})

    @app.get("/open-dingtalk-popup", response_class=HTMLResponse)
    def open_dingtalk_popup(cid: str = "", conversation_id: str = "") -> HTMLResponse:
        if not cid.strip() and not conversation_id.strip():
            return HTMLResponse("missing cid or conversation_id", status_code=400)
        return HTMLResponse(
            render_dingtalk_open_popup(cid=cid, conversation_id=conversation_id)
        )

    @app.get("/open-dingtalk")
    def open_dingtalk(request: Request, cid: str = "", conversation_id: str = "") -> JSONResponse:
        cleaned_conversation_id = conversation_id.strip()
        if cleaned_conversation_id:
            bridge_url = (
                f"{request.url.scheme}://{request.url.netloc}"
                "/dingtalk/open-chat-bridge"
                f"?conversation_id={quote(cleaned_conversation_id, safe='')}"
            )
            dingtalk_url = _dingtalk_pc_slide_link_url(bridge_url)
            completed = subprocess.run(["/usr/bin/open", dingtalk_url], check=False)
            return JSONResponse(
                {
                    "ok": completed.returncode == 0,
                    "dingtalk_url": dingtalk_url,
                    "bridge_url": bridge_url,
                    "open_returncode": completed.returncode,
                }
            )
        cleaned_cid = cid.strip()
        if not cleaned_cid:
            return JSONResponse(
                {"ok": False, "error": "missing_cid"},
                status_code=400,
            )
        dingtalk_url = _dingtalk_conversation_url(cleaned_cid)
        completed = subprocess.run(["/usr/bin/open", dingtalk_url], check=False)
        return JSONResponse(
            {
                "ok": completed.returncode == 0,
                "dingtalk_url": dingtalk_url,
                "open_returncode": completed.returncode,
            }
        )

    @app.get("/attempts/{attempt_id}", response_class=HTMLResponse)
    def attempt_detail(attempt_id: int) -> HTMLResponse:
        status, html = render_attempt_detail(AutoReplyStore(db_path), attempt_id)
        return HTMLResponse(html, status_code=status)

    @app.post("/attempts/{attempt_id}/feedback")
    async def feedback(attempt_id: int, request: Request):
        status, headers, html = handle_feedback_post(
            AutoReplyStore(db_path),
            attempt_id,
            await request.body(),
        )
        return _fastapi_post_response(status, headers, html)

    @app.post("/user-feedback/resolve")
    async def user_feedback_resolve(request: Request):
        status, headers, html = handle_user_feedback_resolve_post(
            AutoReplyStore(db_path),
            await request.body(),
        )
        return _fastapi_post_response(status, headers, html)

    @app.post("/user-feedback/sync")
    def user_feedback_sync():
        status, headers, html = handle_user_feedback_sync_post(AutoReplyStore(db_path))
        return _fastapi_post_response(status, headers, html)

    @app.post("/developer-prompt")
    async def developer_prompt_save(request: Request):
        if request.query_params.get("tab") == "user":
            status, headers, html = handle_user_prompt_post(await request.body())
        else:
            status, headers, html = handle_developer_prompt_post(await request.body())
        return _fastapi_post_response(status, headers, html)

    @app.post("/config")
    async def config_save(request: Request):
        if request.query_params.get("tab") == "user":
            status, headers, html = handle_user_prompt_post(await request.body())
        else:
            status, headers, html = handle_developer_prompt_post(await request.body())
        return _fastapi_post_response(status, headers, html)

    @app.post("/config/variables")
    async def config_variables_save(request: Request):
        status, headers, html = handle_prompt_variables_post(await request.body())
        return _fastapi_post_response(status, headers, html)

    @app.post("/config/system")
    async def config_system_save(request: Request):
        status, headers, html = handle_system_config_post(await request.body())
        return _fastapi_post_response(status, headers, html)

    @app.post("/config/dingtalk/reconnect")
    def config_dingtalk_reconnect():
        status, headers, html = handle_dingtalk_reconnect_post()
        return _fastapi_post_response(status, headers, html)

    @app.get("/api/dingtalk/status")
    def api_dingtalk_status():
        return JSONResponse(dingtalk_connection_status(AutoReplyStore(db_path)))

    @app.post("/attempts/{attempt_id}/recall")
    def recall(attempt_id: int, request: Request):
        status, headers, html = handle_recall_post(
            AutoReplyStore(db_path),
            DwsClient(ding_robot_code=ding_robot_code, ding_robot_name=ding_robot_name),
            attempt_id,
            return_to=request.query_params.get("return_to", ""),
        )
        return _fastapi_post_response(status, headers, html)

    @app.post("/attempts/{attempt_id}/rerun")
    def rerun_attempt(attempt_id: int, request: Request):
        status, headers, html = handle_rerun_attempt_post(
            AutoReplyStore(db_path),
            attempt_id,
            return_to=request.query_params.get("return_to", ""),
        )
        return _fastapi_post_response(status, headers, html)

    @app.post("/messages/reviewed-reply")
    async def reviewed_reply(request: Request):
        payload = json.loads((await request.body()).decode("utf-8"))
        result = handle_reviewed_message_reply(
            AutoReplyStore(db_path),
            DwsClient(ding_robot_code=ding_robot_code, ding_robot_name=ding_robot_name),
            user_name=str(payload["user_name"]),
            group_name=str(payload["group_name"]),
            message_str=str(payload["message_str"]),
            reply_text=str(payload["reply_text"]),
            reviewer_feedback=str(
                payload.get("reviewer_feedback") or payload.get("feedback") or ""
            ),
        )
        return JSONResponse(result)

    return app


def create_default_audit_app() -> FastAPI:
    return create_audit_app(
        _configured_worker_db_path(),
        ding_robot_code=os.getenv("CEO_DING_ROBOT_CODE")
        or os.getenv("DINGTALK_DING_ROBOT_CODE"),
        ding_robot_name=os.getenv("CEO_DING_ROBOT_NAME"),
    )


def run_audit_web(
    db_path: Path,
    host: str,
    port: int,
    ding_robot_code: str | None = None,
    ding_robot_name: str | None = None,
    reload: bool = False,
    reload_delay_seconds: int = 1,
    reload_dirs: list[Path] | None = None,
) -> None:
    print(f"audit-web listening on http://{host}:{port}", flush=True)
    if reload:
        os.environ["CEO_WORKER_DB"] = str(db_path)
        if ding_robot_code:
            os.environ["CEO_DING_ROBOT_CODE"] = ding_robot_code
        if ding_robot_name:
            os.environ["CEO_DING_ROBOT_NAME"] = ding_robot_name
        uvicorn.run(
            "app.audit_web:create_default_audit_app",
            factory=True,
            host=host,
            port=port,
            loop="asyncio",
            http="h11",
            reload=True,
            reload_delay=reload_delay_seconds,
            reload_dirs=[str(path) for path in reload_dirs] if reload_dirs else None,
        )
        return

    uvicorn.run(
        create_audit_app(
            db_path,
            ding_robot_code=ding_robot_code,
            ding_robot_name=ding_robot_name,
        ),
        host=host,
        port=port,
        loop="asyncio",
        http="h11",
    )


def _fastapi_post_response(status: int, headers: dict[str, str], html: str):
    if status == 303:
        return RedirectResponse(headers["Location"], status_code=303)
    return HTMLResponse(html, status_code=status)


def _positive_int_query(request: Request, name: str, *, default: int) -> int:
    raw_value = request.query_params.get(name, "")
    try:
        value = int(raw_value)
    except ValueError:
        return default
    return value if value > 0 else default


def _attempt_detail_body(
    attempt: ReplyAttempt,
    sent_reply: SentReply | None,
    codex_session_id: str | None,
    feedback_events: list[FeedbackEvent],
) -> str:
    fields = [
        ("触发消息 ID", attempt.trigger_message_id),
        ("动作", attempt.action),
        ("敏感度", attempt.sensitivity_kind),
        ("权限判断", _permission_display(attempt)),
        ("发送状态", attempt.send_status),
        ("发送错误", attempt.send_error),
        ("重试次数", str(attempt.retry_count)),
        ("创建时间", _format_local_time(attempt.created_at)),
        ("更新时间", _format_local_time(attempt.updated_at)),
        ("复核时间", _format_local_time(attempt.reviewed_at or "")),
    ]
    heading_subtitle = " · ".join(
        part
        for part in (
            attempt.conversation_title.strip(),
            attempt.trigger_sender.strip(),
            _format_local_time(attempt.created_at),
        )
        if part
    )
    return (
        f"{_detail_page_header(f'事件详情 #{attempt.id}', heading_subtitle, '/', '返回处理记录')}"
        f"{_attempt_conversation_banner(attempt, sent_reply, codex_session_id)}"
        f"{_attempt_detail_grid(fields)}"
        f"{_review_panel(attempt, sent_reply, feedback_events)}"
        f"{_feedback_dialog(attempt)}"
        f"{_quality_warning_card(attempt)}"
        f"{_context_only_info_card(attempt)}"
        f"{_oa_metadata_card(attempt)}"
        f"{_calendar_metadata_card(attempt)}"
        f"{_text_card('审计摘要', attempt.audit_summary)}"
        f"{_audit_tool_uses_card(attempt)}"
        f"{_text_card('回复草稿（原始生成）', attempt.draft_reply_text)}"
    )


def _permission_display(attempt: ReplyAttempt) -> str:
    action = attempt.permission_action.strip()
    reason = attempt.permission_reason.strip()
    if action and reason:
        return f"{action} · {reason}"
    return action or reason


def _attempt_conversation_banner(
    attempt: ReplyAttempt,
    sent_reply: SentReply | None,
    codex_session_id: str | None,
) -> str:
    subtitle = (
        f"<div class=\"attempt-conversation-sub\">触发人：{escape(attempt.trigger_sender)}</div>"
        if attempt.trigger_sender.strip()
        else ""
    )
    agent_log = (
        f"<a class=\"agent-log-button\" href=\"/codex/{escape(codex_session_id)}\">"
        "查看执行会话</a>"
        if codex_session_id
        else "<span class=\"muted\">暂无执行会话</span>"
    )
    return (
        "<section class=\"card compact-card attempt-conversation-banner\">"
        "<div class=\"attempt-conversation-left\">"
        "<div class=\"attempt-conversation-label\">群名</div>"
        "<div class=\"attempt-conversation-main\">"
        f"<div class=\"attempt-conversation-title\">{escape(attempt.conversation_title)}</div>"
        f"{subtitle}"
        "</div>"
        "</div>"
        "<div class=\"attempt-banner-actions\">"
        f"{agent_log}"
        f"{_attempt_row_actions(attempt, sent_reply, show_feedback_action=True)}"
        "</div>"
        "</section>"
    )


def _attempt_detail_grid(fields: list[tuple[str, str]]) -> str:
    cells = "".join(
        "<div class=\"attempt-detail-cell\">"
        f"<div class=\"attempt-detail-label\">{escape(label)}</div>"
        f"<div class=\"attempt-detail-value\">{escape(value)}</div>"
        "</div>"
        for label, value in fields
    )
    return (
        "<section class=\"card compact-card\">"
        f"<div class=\"attempt-detail-grid\">{cells}</div>"
        "</section>"
    )


def _sync_feedback_events_for_sent_replies(
    store: AutoReplyStore,
    sent_replies: Iterable[SentReply],
) -> None:
    sync_feedback_events_for_sent_replies_impl(store, sent_replies)


def _sync_feedback_events_for_context(
    store: AutoReplyStore,
    context: FeedbackLinkContext,
) -> None:
    sync_feedback_events_for_context_impl(store, context)


def _feedback_context_for_sent_reply(
    sent_reply: SentReply,
) -> FeedbackLinkContext | None:
    return feedback_context_for_sent_reply(sent_reply)


def _feedback_token_for_sent_reply(sent_reply: SentReply | None) -> str:
    if sent_reply is None:
        return ""
    if sent_reply.feedback_token.strip():
        return sent_reply.feedback_token.strip()
    context = extract_feedback_link_context(sent_reply.reply_text)
    return context.feedback_token if context else ""


def _feedback_events_by_sent_reply(
    store: AutoReplyStore,
    sent_replies: Iterable[SentReply],
) -> dict[str, list[FeedbackEvent]]:
    tokens = [_feedback_token_for_sent_reply(sent_reply) for sent_reply in sent_replies]
    return store.list_feedback_events_for_tokens(tokens)


def _feedback_events_for_sent_reply(
    sent_reply: SentReply | None,
    feedback_events_by_token: dict[str, list[FeedbackEvent]],
) -> list[FeedbackEvent]:
    token = _feedback_token_for_sent_reply(sent_reply)
    if not token:
        return []
    return feedback_events_by_token.get(token, [])


def _attempt_feedback_summary(
    feedback_events: list[FeedbackEvent],
    sent_reply: SentReply | None,
) -> str:
    if feedback_events:
        latest = feedback_events[0]
        label = _feedback_rating_stars(latest) or latest.rating_label or latest.rating
        comment = f" | {_excerpt(latest.comment, 90)}" if latest.comment.strip() else ""
        return (
            "<div class=\"attempt-foot\">"
            f"<span class=\"feedback-chip\">反馈：{escape(label)}{escape(comment)}</span>"
            "</div>"
        )
    return ""


def _feedback_rating_stars(event: FeedbackEvent) -> str:
    return _feedback_rating_stars_for_rating(event.rating)


def _feedback_rating_stars_for_rating(rating: str) -> str:
    star_counts = {
        "very_unhelpful": 1,
        "not_useful": 2,
        "neutral": 3,
        "useful": 4,
        "very_useful": 5,
    }
    count = star_counts.get(rating)
    return "☆" * count if count else ""


def _counterparty_feedback_card(
    sent_reply: SentReply | None,
    feedback_events: list[FeedbackEvent],
) -> str:
    token = _feedback_token_for_sent_reply(sent_reply)
    if not token and not feedback_events:
        return ""
    if not feedback_events:
        return (
            "<section class=\"card feedback-card\"><h2>对方反馈</h2>"
            "<p class=\"muted\">还没有收到对方反馈。</p>"
            f"<p class=\"feedback-token\">token: {escape(token)}</p></section>"
        )
    events_html = "".join(_feedback_event_html(event) for event in feedback_events)
    return (
        "<section class=\"card feedback-card\"><h2>对方反馈</h2>"
        f"<p class=\"feedback-token\">token: {escape(token)}</p>"
        f"{events_html}</section>"
    )


_DWS_MESSAGE_ID_KEYS = {
    "openMessageId",
    "open_message_id",
    "messageId",
    "message_id",
    "msgId",
    "msg_id",
    "openMsgId",
    "open_msg_id",
}
_DWS_OPEN_TASK_ID_KEYS = {
    "openTaskId",
    "open_task_id",
    "open_taskId",
}


def _sent_reply_send_result_payload(sent_reply: SentReply | None) -> dict[str, object]:
    if sent_reply is None or not sent_reply.send_result_json.strip():
        return {}
    try:
        payload = json.loads(sent_reply.send_result_json)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _find_string_value(payload: object, keys: set[str]) -> str:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key in keys and isinstance(value, str) and value.strip():
                return value.strip()
        for value in payload.values():
            found = _find_string_value(value, keys)
            if found:
                return found
    if isinstance(payload, list):
        for item in payload:
            found = _find_string_value(item, keys)
            if found:
                return found
    return ""


def _sent_reply_recall_message_id(sent_reply: SentReply | None) -> str:
    return _find_string_value(
        _sent_reply_send_result_payload(sent_reply),
        _DWS_MESSAGE_ID_KEYS,
    )


def _sent_reply_open_task_id(sent_reply: SentReply | None) -> str:
    return _find_string_value(
        _sent_reply_send_result_payload(sent_reply),
        _DWS_OPEN_TASK_ID_KEYS,
    )


def _sent_reply_has_recall_target(sent_reply: SentReply | None) -> bool:
    if sent_reply is None:
        return False
    return bool(
        _sent_reply_recall_message_id(sent_reply)
        or _sent_reply_open_task_id(sent_reply)
        or sent_reply.recall_key.strip()
    )


def _recall_card(attempt: ReplyAttempt, sent_reply: SentReply | None) -> str:
    if sent_reply is None:
        return ""
    status = sent_reply.recall_status.strip().lower()
    status_html = ""
    if status == "recalled":
        recalled_at = _format_local_time(sent_reply.recalled_at or "")
        return (
            "<section class=\"card recall-card\"><h2>撤销发送</h2>"
            f"<p><span class=\"pill status-sent\">已撤销</span> "
            f"<span class=\"muted\">{escape(recalled_at)}</span></p></section>"
        )
    if status == "failed":
        status_html = (
            "<p><span class=\"pill status-failed\">上次撤销失败</span></p>"
            f"<pre class=\"mini-pre\">{escape(sent_reply.recall_error)}</pre>"
        )
    if not _sent_reply_has_recall_target(sent_reply):
        return status_html and (
            "<section class=\"card recall-card\"><h2>撤销发送</h2>"
            f"{status_html}"
            "<p class=\"muted\">没有可撤销消息 ID 或 key。</p></section>"
        )
    return (
        "<section class=\"card recall-card\"><h2>撤销发送</h2>"
        "<p class=\"muted\">撤回这条处理记录已发送到钉钉的回复。</p>"
        f"{status_html}"
        f"<form method=\"post\" action=\"/attempts/{attempt.id}/recall\" "
        "onsubmit=\"return confirm('确认撤销这条已发送消息？')\">"
        "<button class=\"danger\" type=\"submit\">撤销发送</button>"
        "</form></section>"
    )


def _rerun_card(attempt: ReplyAttempt) -> str:
    return (
        "<section class=\"card compact-card rerun-card\">"
        "<h2>重新处理</h2>"
        "<p class=\"muted\">用当前代码和 prompt 重新处理原触发消息。"
        "可能实际发送回复、处理日历或执行审批。</p>"
        f"<form method=\"post\" action=\"/attempts/{attempt.id}/rerun\" "
        "onsubmit=\"return confirm('确认重新处理这条记录？可能会实际发送新回复或执行日历/OA 动作。')\">"
        "<button type=\"submit\">重新处理</button>"
        "</form></section>"
    )


def _feedback_event_html(event: FeedbackEvent) -> str:
    rating = event.rating_label or event.rating or "feedback"
    comment = event.comment.strip() or "未填写评语"
    return (
        "<article class=\"feedback-event\">"
        "<div class=\"feedback-event-head\">"
        f"<span class=\"feedback-rating\">{escape(rating)}</span>"
        f"<time class=\"attempt-time\">{escape(_format_local_time(event.received_at or event.updated_at))}</time>"
        "</div>"
        f"<div class=\"feedback-comment\">{escape(comment)}</div>"
        f"<p class=\"muted\">source: {escape(event.source)}</p>"
        "</article>"
    )


def _review_panel(
    attempt: ReplyAttempt,
    sent_reply: SentReply | None,
    feedback_events: list[FeedbackEvent],
) -> str:
    reply_text = attempt.final_reply_text or attempt.draft_reply_text
    if not reply_text.strip():
        reply_text = _reaction_display_text(attempt) or "暂无生成回复记录。"
    side_cards = _counterparty_feedback_card(sent_reply, feedback_events)
    side_html = (
        "<div class=\"review-side\">"
        f"{side_cards}"
        "</div>"
        if side_cards
        else ""
    )
    grid_class = "review-grid" if side_cards else "review-grid single"
    return (
        f"<section class=\"{grid_class}\">"
        "<div class=\"card\">"
        "<div class=\"reply-meta\">"
        f"{_attempt_action_pills(attempt)}"
        "</div>"
        "<h2>触发消息</h2>"
        f"<pre class=\"trigger-pre\">{escape(_trigger_text(attempt))}</pre>"
        "<h2>判断依据</h2>"
        f"<div class=\"codex-reason\">{escape(attempt.codex_reason)}</div>"
        "<h2>生成回复</h2>"
        f"<pre class=\"reply-pre\">{escape(reply_text)}</pre>"
        "</div>"
        f"{side_html}"
        "</section>"
    )


def _quality_warning_card(attempt: ReplyAttempt) -> str:
    warnings = _quality_warnings(attempt)
    if not warnings:
        return ""
    items = "".join(f"<li>{escape(warning)}</li>" for warning in warnings)
    return (
        "<section class=\"card quality-warning\"><h2>审计质量提醒</h2>"
        f"<ul>{items}</ul></section>"
    )


def _context_only_info_card(attempt: ReplyAttempt) -> str:
    info_icon = _attempt_info_icon(attempt)
    if not info_icon:
        return ""
    return (
        "<section class=\"card compact-card\">"
        f"<h2 class=\"context-only-info\">审计上下文 {info_icon}</h2>"
        "</section>"
    )


def _oa_metadata_card(attempt: ReplyAttempt) -> str:
    if not any(
        value.strip()
        for value in (
            attempt.oa_process_instance_id,
            attempt.oa_task_id,
            attempt.oa_url,
            attempt.oa_action,
            attempt.oa_remark,
            attempt.oa_action_result_json,
        )
    ):
        return ""
    rows = "".join(
        f"<div class=\"muted\">{escape(label)}</div><div>{escape(value)}</div>"
        for label, value in (
            ("process instance", attempt.oa_process_instance_id),
            ("task id", attempt.oa_task_id),
            ("url", attempt.oa_url),
            ("action", attempt.oa_action),
            ("remark", attempt.oa_remark),
        )
    )
    return (
        "<section class=\"card compact-card\"><h2>OA approval</h2>"
        f"<div class=\"grid\">{rows}</div></section>"
        f"{_json_card('OA action result', attempt.oa_action_result_json)}"
    )


def _calendar_metadata_card(attempt: ReplyAttempt) -> str:
    if not any(
        value.strip()
        for value in (
            attempt.calendar_event_id,
            attempt.calendar_response_status,
            attempt.calendar_response_result_json,
        )
    ):
        return ""
    rows = "".join(
        f"<div class=\"muted\">{escape(label)}</div><div>{escape(value)}</div>"
        for label, value in (
            ("event id", attempt.calendar_event_id),
            ("response", attempt.calendar_response_status),
        )
    )
    return (
        "<section class=\"card compact-card\"><h2>Calendar response</h2>"
        f"<div class=\"grid\">{rows}</div></section>"
        + (
            _json_card(
                "Calendar response result",
                attempt.calendar_response_result_json,
            )
            if attempt.calendar_response_result_json.strip()
            else ""
        )
    )


def _attempt_action_pills(attempt: ReplyAttempt) -> str:
    calendar_only = (
        attempt.send_status.strip().lower() == "calendar"
        and attempt.calendar_response_status.strip()
    )
    actions = [] if calendar_only else [_send_status_action(attempt)]
    if attempt.oa_action.strip():
        actions.append(
            (
                attempt.oa_action.strip(),
                attempt.oa_action,
                _oa_action_icon(attempt.oa_action),
            )
        )
    if attempt.calendar_response_status.strip():
        actions.append(
            (
                _display_action_state(attempt.calendar_response_status),
                attempt.calendar_response_status,
                _calendar_status_icon(attempt.calendar_response_status),
            )
        )
    return "".join(_status_action_pill(label, state, icon) for label, state, icon in actions)


def _attempt_action_label_text(attempt: ReplyAttempt) -> str:
    calendar_only = (
        attempt.send_status.strip().lower() == "calendar"
        and attempt.calendar_response_status.strip()
    )
    return " · ".join(
        label
        for label in (
            (
                ""
                if calendar_only
                else _send_status_action(attempt)[0]
            ),
            (
                attempt.oa_action.strip()
                if attempt.oa_action.strip()
                else ""
            ),
            (
                _display_action_state(attempt.calendar_response_status)
                if attempt.calendar_response_status.strip()
                else ""
            ),
        )
        if label
    )


def _display_action_state(value: str) -> str:
    normalized = value.strip().lower().replace("-", "_")
    mapped = {
        "sent": "已发送",
        "skipped": "已跳过",
        "failed": "失败",
        "processing": "处理中",
        "pending": "待处理",
        "dry_run": "预演",
        "dryrun": "预演",
        "reacted": "已表态",
        "calendar": "日历处理",
        "accepted": "已接受",
        "tentative": "暂定",
        "declined": "已拒绝",
        "approved": "已通过",
        "commented": "已评论",
        "returned": "已退回",
        "rejected": "已拒绝",
        "blocked": "已阻塞",
    }
    if normalized in mapped:
        return mapped[normalized]
    return " ".join(
        part.capitalize()
        for part in normalized.split("_")
        if part
    )


def _send_status_action(attempt: ReplyAttempt) -> tuple[str, str, str]:
    send_status = attempt.send_status
    if send_status.strip().lower() == "reacted":
        return "已表态", send_status, "smile"
    icon = "circle-alert" if send_status.strip().lower() in {"failed", "blocked"} else "message-circle"
    return _display_action_state(send_status), send_status, icon


def _calendar_status_icon(value: str) -> str:
    normalized = value.strip().lower().replace("_", "-")
    if normalized in {"decline", "declined"}:
        return "calendar-x"
    if normalized in {"tentative", "maybe"}:
        return "calendar-clock"
    return "calendar-check"


def _oa_action_icon(value: str) -> str:
    normalized = {
        "退回": "returned",
        "评论": "commented",
        "留言": "commented",
    }.get(value.strip(), value.strip().lower().replace("_", "-"))
    if normalized in {"return", "returned"}:
        normalized = "returned"
    elif normalized in {"comment", "commented"}:
        normalized = "commented"
    if normalized in {"commented", "returned"}:
        return "clipboard-pen-line"
    return "clipboard-check"


def _action_state_class(value: str) -> str:
    normalized = value.strip().lower().replace("_", "-")
    mapped = {
        "通过": "approved",
        "同意": "approved",
        "拒绝": "rejected",
        "退回": "returned",
        "评论": "commented",
        "留言": "commented",
    }.get(value.strip(), normalized)
    if mapped in {"approve", "approved", "pass"}:
        mapped = "approved"
    elif mapped in {"accept", "accepted"}:
        mapped = "accepted"
    elif mapped in {"decline", "declined"}:
        mapped = "declined"
    elif mapped in {"reject", "rejected"}:
        mapped = "rejected"
    elif mapped in {"return", "returned"}:
        mapped = "returned"
    elif mapped in {"dry-run", "dryrun"}:
        mapped = "dry-run"
    safe = "".join(
        char if (char.isascii() and char.isalnum()) or char == "-" else "-"
        for char in mapped
    )
    safe = "-".join(part for part in safe.split("-") if part)
    return f"action-state-{safe or 'unknown'}"


def _quality_warnings(attempt: ReplyAttempt) -> list[str]:
    if attempt.send_status == "skipped":
        return []
    warnings: list[str] = []
    if not attempt.audit_summary.strip():
        warnings.append("missing audit_summary")
    return warnings


def _attempt_warning_summary(attempt: ReplyAttempt) -> str:
    warnings = _quality_warnings(attempt)
    if not warnings:
        return ""
    if len(warnings) == 1:
        return f"质量提醒：{warnings[0]}"
    return f"质量提醒：{len(warnings)} 条"


def _attempt_info_icon(attempt: ReplyAttempt) -> str:
    tooltip = _attempt_info_tooltip(attempt)
    if not tooltip:
        return ""
    escaped_tooltip = escape(tooltip)
    return (
        f"<span class=\"attempt-info\" data-tooltip=\"{escaped_tooltip}\" "
        f"aria-label=\"{escaped_tooltip}\" tabindex=\"0\">i</span>"
    )


def _attempt_info_tooltip(attempt: ReplyAttempt) -> str:
    if attempt.send_status == "skipped" or attempt.action not in {
        "send_reply",
        "ask_clarifying_question",
    }:
        return ""
    notes: list[str] = []
    if not attempt.codex_session_id.strip():
        notes.append(NO_CODEX_SESSION_TOOLTIP)
    has_documents = _json_array_has_items(
        attempt.audit_documents_json
    ) or audit_summary_explains_no_documents(attempt.audit_summary)
    has_tool_events = _json_array_has_items(attempt.audit_tool_events_json)
    if not has_documents and not has_tool_events:
        notes.append(NO_AUDIT_CONTEXT_TOOLTIP)
    elif not has_documents:
        notes.append(NO_AUDIT_DOCUMENTS_TOOLTIP)
    elif not has_tool_events:
        notes.append(CONTEXT_ONLY_TOOLTIP)
    return " ".join(notes)


def _json_array_has_items(text: str) -> bool:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return False
    return isinstance(payload, list) and len(payload) > 0


def _related_history_card(
    attempts: list[ReplyAttempt],
    *,
    session_id: str = "",
    store: AutoReplyStore | None = None,
) -> str:
    if not attempts:
        return (
            "<section class=\"card\"><h2>关联处理记录</h2>"
            "<p class=\"muted\">这个执行会话暂无关联处理记录。</p>"
            "</section>"
        )
    rows = []
    for attempt in attempts:
        sent_reply = (
            store.get_sent_reply(attempt.conversation_id, attempt.trigger_message_id)
            if store is not None
            else None
        )
        rows.append(
            "<tr>"
            f"<td>{_attempt_link(attempt)}</td>"
            f"<td>{escape(_format_local_time(attempt.created_at))}</td>"
            f"<td>{escape(attempt.trigger_sender)}</td>"
            f"<td>{_attempt_action_pills(attempt)}</td>"
            f"<td>{escape(_excerpt(attempt.trigger_text, 120))}</td>"
            f"<td>{_attempt_row_actions(attempt, sent_reply, session_id=session_id)}</td>"
            "</tr>"
        )
    return (
        "<section class=\"card\"><h2>关联处理记录</h2>"
        "<table><thead><tr><th>记录</th><th>时间</th><th>发送人</th>"
        "<th>结果</th><th>触发消息</th><th>操作</th></tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table></section>"
    )


def _attempt_row_actions(
    attempt: ReplyAttempt,
    sent_reply: SentReply | None,
    *,
    session_id: str = "",
    show_feedback_action: bool = False,
) -> str:
    return_to = f"/codex/{quote(session_id, safe='')}" if session_id else f"/attempts/{attempt.id}"
    return_to_query = quote(return_to, safe="/")
    dingtalk_href = (
        "/open-dingtalk-popup?"
        f"conversation_id={quote(attempt.conversation_id, safe='')}"
    )
    recall_html = (
        f"<form method=\"post\" action=\"/attempts/{attempt.id}/recall?return_to={return_to_query}\" "
        "onsubmit=\"return confirm('确认撤销这条已发送消息？')\">"
        "<button class=\"danger\" type=\"submit\">撤销发送</button>"
        "</form>"
        if _sent_reply_has_recall_target(sent_reply)
        else "<span class=\"disabled-action\" title=\"没有可撤销消息 ID 或 key\">撤销发送</span>"
    )
    feedback_action = (
        "<button class=\"feedback-modal-action\" type=\"button\" "
        "onclick=\"document.getElementById('internal-feedback-dialog')?.showModal()\">"
        "内部反馈</button>"
        if show_feedback_action
        else ""
    )
    return (
        "<div class=\"attempt-row-actions\">"
        f"<form method=\"post\" action=\"/attempts/{attempt.id}/rerun?return_to={return_to_query}\" "
        "onsubmit=\"return confirm('确认重新处理这条记录？可能会实际发送新回复或执行日历/OA 动作。')\">"
        "<button class=\"rerun\" type=\"submit\">重新处理</button>"
        "</form>"
        f"{recall_html}"
        f"<a class=\"compact-button open-dingtalk-action\" href=\"{dingtalk_href}\" "
        "onclick=\"window.open(this.href,'ceo-open-dingtalk','popup,width=420,height=260'); return false;\" "
        "target=\"ceo-open-dingtalk\" rel=\"noopener\">查看钉钉消息</a>"
        f"{feedback_action}"
        "</div>"
    )


def _codex_event_card(event: RenderedCodexEvent) -> str:
    open_attr = " open" if event.expanded else ""
    preview = _excerpt(event.body, 140)
    return (
        f"<details class=\"event event-{escape(event.kind)}\"{open_attr}>"
        "<summary>"
        "<div>"
        f"<div class=\"event-title\">{escape(event.title)}</div>"
        f"<div class=\"event-preview\">{escape(preview)}</div>"
        "</div>"
        f"<time>{escape(event.timestamp)}</time>"
        "</summary>"
        f"<pre>{escape(event.body)}</pre>"
        "</details>"
    )


def _feedback_dialog(attempt: ReplyAttempt) -> str:
    return (
        "<dialog id=\"internal-feedback-dialog\" class=\"feedback-dialog\">"
        "<section class=\"feedback-dialog-card\" id=\"feedback\">"
        "<div class=\"feedback-dialog-head\">"
        "<div><h2>内部反馈/建议修改</h2>"
        "<p class=\"muted\">记录这条判断哪里不对，或给出更合适的回复。</p></div>"
        "<form method=\"dialog\"><button class=\"feedback-dialog-close\" "
        "aria-label=\"关闭内部反馈弹窗\">×</button></form>"
        "</div>"
        f"<form method=\"post\" action=\"/attempts/{attempt.id}/feedback\">"
        "<label>反馈意见</label><textarea name=\"feedback\" "
        "placeholder=\"这条判断哪里不对、为什么不满意、以后应该遵守什么规则\">"
        f"{escape(attempt.reviewer_feedback)}</textarea>"
        "<label>建议回复</label><textarea name=\"corrected_reply\" "
        "placeholder=\"如果重写，这条消息应该怎么回复\">"
        f"{escape(attempt.corrected_reply_text)}</textarea>"
        "<div class=\"feedback-dialog-actions\">"
        "<button class=\"feedback-dialog-cancel\" type=\"button\" "
        "onclick=\"document.getElementById('internal-feedback-dialog')?.close()\">取消</button>"
        "<button type=\"submit\">保存反馈</button>"
        "</div></form></section></dialog>"
    )


def _review_link(attempt: ReplyAttempt) -> str:
    label = "查看/反馈" if not (attempt.reviewer_feedback or attempt.corrected_reply_text) else "查看/修改"
    return f"<a class=\"review-link\" href=\"/attempts/{attempt.id}\">{label}</a>"


def _attempt_link(attempt: ReplyAttempt) -> str:
    return (
        f"<a href=\"/attempts/{attempt.id}\">"
        f"#{attempt.id} · {escape(_attempt_action_label_text(attempt))}</a>"
    )


def _attempt_text_line(label: str, text: str, length: int) -> str:
    return (
        "<div class=\"attempt-line\">"
        f"<span class=\"attempt-label\">{escape(label)}</span>"
        f"<span class=\"attempt-copy\">{escape(_excerpt(text, length))}</span>"
        "</div>"
    )


def _attempt_reply_line(attempt: ReplyAttempt) -> str:
    reaction = _reaction_display_text(attempt)
    if reaction:
        return (
            "<div class=\"attempt-line\">"
            "<span class=\"attempt-label\">答</span>"
            f"<span class=\"attempt-copy attempt-reaction-copy\">{escape(reaction)}</span>"
            "</div>"
        )
    return _attempt_text_line("答", _reply_preview_text(attempt), 320)


def _reply_preview_text(attempt: ReplyAttempt) -> str:
    text = attempt.final_reply_text or attempt.draft_reply_text
    if not text.strip():
        return _reaction_display_text(attempt)
    lines = text.splitlines()
    while lines and (not lines[0].strip() or lines[0].lstrip().startswith(">")):
        lines.pop(0)
    preview = "\n".join(lines).strip()
    return preview or text


def _reaction_display_text(attempt: ReplyAttempt) -> str:
    if attempt.send_status.strip().lower() != "reacted":
        return ""
    summary = attempt.send_error.strip()
    if not summary or summary == "message_reaction":
        return ""
    values = []
    for part in summary.split(", "):
        kind, separator, value = part.partition(":")
        if separator and kind.strip().lower() in {"emoji", "text_emotion"}:
            value = value.strip()
            if value:
                values.append(value)
    return " ".join(values) if values else summary


def _text_card(title: str, text: str) -> str:
    return f"<section class=\"card\"><h2>{escape(title)}</h2><pre>{escape(text)}</pre></section>"


def _json_card(title: str, text: str) -> str:
    return (
        f"<section class=\"card\"><h2>{escape(title)}</h2>"
        f"<pre class=\"json-pre\">{_json_html(text)}</pre></section>"
    )


def _collapsible_json_card(title: str, text: str) -> str:
    return (
        "<details class=\"card collapsible-card\">"
        f"<summary><h2>{escape(title)}</h2></summary>"
        f"<pre class=\"json-pre\">{_json_html(text)}</pre></details>"
    )


def _audit_tool_uses_card(attempt: ReplyAttempt) -> str:
    uses = _audit_tool_uses_for_attempt(attempt)
    if not uses:
        return _collapsible_json_card("Tool uses", "[]")
    document_count = sum(1 for use in uses if str(use.get("tool") or "") == "document")
    call_count = len(uses) - document_count
    return (
        "<details class=\"card collapsible-card\">"
        "<summary><h2>Tool uses</h2>"
        "<span class=\"audit-tool-count\">"
        f"{len(uses)} total · {call_count} calls · {document_count} documents"
        "</span></summary>"
        f"<div class=\"audit-tool-list\">{_audit_tool_uses_html(uses)}</div>"
        "</details>"
    )


def _audit_tool_uses_for_attempt(attempt: ReplyAttempt) -> list[dict[str, object]]:
    return [
        *_audit_document_uses_for_attempt(attempt),
        *_audit_event_uses_for_attempt(attempt),
    ]


def _audit_document_uses_for_attempt(attempt: ReplyAttempt) -> list[dict[str, object]]:
    documents = _json_list(attempt.audit_documents_json)
    uses: list[dict[str, object]] = []
    for document in documents:
        if not isinstance(document, dict):
            continue
        title = _first_nonempty_string(
            document,
            ("title", "name", "path", "url", "mcp_name", "tool"),
        )
        source = _audit_source_text(document)
        args = _audit_explicit_args_payload(document)
        uses.append(
            {
                "title": title or "审计材料",
                "tool": _first_nonempty_string(document, ("tool", "type")) or "document",
                "relevance": _first_nonempty_string(
                    document,
                    ("relevance", "reason", "description"),
                ),
                "source": source,
                "format": _audit_format_text(document, args),
                "args": args,
                "output": _first_nonempty_string(document, ("output", "content")),
                "call_id": _first_nonempty_string(document, ("call_id",)),
            }
        )
    return uses


def _audit_event_uses_for_attempt(attempt: ReplyAttempt) -> list[dict[str, object]]:
    events = _audit_tool_events_for_attempt(attempt)
    calls: list[dict[str, object]] = []
    by_call_id: dict[str, dict[str, object]] = {}
    for event in events:
        tool = str(event.get("tool") or "tool").strip() or "tool"
        call_id = str(event.get("call_id") or "").strip()
        if tool == "tool_output":
            target = by_call_id.get(call_id) if call_id else None
            if target is not None:
                target["output"] = str(event.get("output") or "")
                if not target.get("source"):
                    target["source"] = _audit_source_text(event)
                continue
            calls.append(
                {
                    "title": _audit_tool_title(event, "Tool output"),
                    "tool": "tool_output",
                    "call_id": call_id,
                    "relevance": _audit_relevance_text(event),
                    "source": _audit_source_text(event),
                    "args": _audit_args_payload(event),
                    "format": _audit_format_text(event, _audit_args_payload(event)),
                    "output": str(event.get("output") or ""),
                }
            )
            continue
        args = _audit_args_payload(event)
        call = {
            "title": _audit_tool_title(event, tool),
            "tool": tool,
            "call_id": call_id,
            "relevance": _audit_relevance_text(event),
            "source": _audit_source_text(event),
            "args": args,
            "format": _audit_format_text(event, args),
            "output": "",
        }
        calls.append(call)
        if call_id:
            by_call_id[call_id] = call
    return calls


def _audit_tool_events_for_attempt(attempt: ReplyAttempt) -> list[dict[str, str]]:
    if attempt.codex_session_id.strip():
        session_events = extract_codex_audit_events_from_session(
            attempt.codex_session_id.strip(),
            start_line=attempt.codex_transcript_start_line,
            end_line=(
                attempt.codex_transcript_end_line
                if attempt.codex_transcript_end_line > 0
                else None
            ),
        )
        if session_events:
            return session_events
    try:
        payload = json.loads(attempt.audit_tool_events_json or "[]")
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    return [event for event in payload if isinstance(event, dict)]


def _audit_tool_uses_html(uses: list[dict[str, object]]) -> str:
    return "".join(_audit_tool_use_html(index, use) for index, use in enumerate(uses, 1))


def _audit_tool_use_html(index: int, use: dict[str, object]) -> str:
    title = str(use.get("title") or "Tool use").strip()
    tool = str(use.get("tool") or "").strip()
    call_id = str(use.get("call_id") or "").strip()
    call_id_line = (
        f"<span class=\"pill\">{escape(call_id)}</span>" if call_id else ""
    )
    tool_line = f"<span class=\"pill\">{escape(tool)}</span>" if tool else ""
    metadata = _audit_tool_metadata_html(use)
    return (
        "<div class=\"audit-tool-event\">"
        "<div class=\"audit-tool-head\">"
        "<div class=\"audit-tool-title\">"
        f"<span class=\"audit-tool-index\">#{index}</span>"
        f"<span>{escape(title)}</span>"
        f"{tool_line}"
        f"{call_id_line}"
        "</div>"
        "</div>"
        f"{metadata}"
        "<div class=\"audit-tool-io\">"
        f"{_audit_tool_args_html(use.get('args'))}"
        f"{_audit_tool_output_html(str(use.get('output') or ''))}"
        "</div>"
        "</div>"
    )


def _audit_tool_metadata_html(use: dict[str, object]) -> str:
    rows = []
    for label, key in (
        ("relevance", "relevance"),
        ("source", "source"),
        ("format", "format"),
    ):
        value = str(use.get(key) or "").strip()
        if value:
            rows.append(
                f"<div class=\"audit-tool-meta-label\">{escape(label)}</div>"
                f"<div class=\"audit-tool-meta-value\">{escape(value)}</div>"
            )
    if not rows:
        return ""
    return (
        "<div class=\"audit-tool-meta\">"
        f"{''.join(rows)}"
        "</div>"
    )


def _audit_tool_args_html(value: object) -> str:
    if value in (None, "", {}, []):
        return ""
    return (
        "<div class=\"audit-tool-section audit-tool-args\">"
        "<div class=\"audit-tool-label\">args</div>"
        f"<div class=\"audit-tool-pre\">{_audit_render_payload_html(value)}</div>"
        "</div>"
    )


def _audit_tool_output_html(text: str) -> str:
    if not text.strip():
        return ""
    preview = _audit_output_preview(text)
    return (
        "<details class=\"audit-tool-output\">"
        "<summary>"
        "<span class=\"audit-tool-label\">output</span>"
        f"<span class=\"audit-tool-output-preview\">{escape(preview)}</span>"
        "</summary>"
        f"<div class=\"audit-tool-output-body\">{_audit_render_payload_html(text)}</div>"
        "</details>"
    )


def _audit_tool_title(event: dict[str, str], default: str) -> str:
    return _first_nonempty_string(event, ("title", "name", "command")) or default


def _audit_relevance_text(event: dict[str, str]) -> str:
    return _first_nonempty_string(event, ("relevance", "reason", "description"))


def _audit_source_text(payload: Mapping[str, object]) -> str:
    values = []
    for key in ("path", "url", "mcp_name", "tool", "command"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            values.append(value.strip())
    return " · ".join(dict.fromkeys(values))


def _audit_args_payload(payload: Mapping[str, object]) -> object:
    explicit = _audit_explicit_args_payload(payload)
    if explicit not in (None, "", {}, []):
        return explicit
    values = {
        key: payload[key]
        for key in ("command", "path", "url", "mcp_name")
        if isinstance(payload.get(key), str) and str(payload[key]).strip()
    }
    return values


def _audit_explicit_args_payload(payload: Mapping[str, object]) -> object:
    args = payload.get("args")
    if args not in (None, "", {}, []):
        return args
    input_text = str(payload.get("input") or "").strip()
    if input_text:
        return _decode_json_text(input_text) or input_text
    return {}


def _audit_format_text(payload: Mapping[str, object], args: object) -> str:
    explicit = _first_nonempty_string(payload, ("format", "output_format", "content_type"))
    if explicit:
        return explicit
    command = _first_nonempty_string(payload, ("command",))
    if command:
        command_format = _command_format(command)
        return f"terminal/{command_format}" if command_format else "terminal"
    tool = _first_nonempty_string(payload, ("tool", "mcp_name"))
    if tool.startswith("memory_"):
        return "mcp/json"
    if isinstance(args, (dict, list)):
        return "json"
    return ""


def _command_format(command: str) -> str:
    pieces = command.split()
    for index, piece in enumerate(pieces[:-1]):
        if piece == "--format" and pieces[index + 1]:
            return pieces[index + 1].strip("'\"")
    return ""


def _first_nonempty_string(payload: Mapping[str, object], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _json_list(text: str) -> list[object]:
    try:
        payload = json.loads(text or "[]")
    except json.JSONDecodeError:
        return []
    return payload if isinstance(payload, list) else []


def _audit_render_payload_html(value: object) -> str:
    expanded = _expand_nested_json_strings(value)
    if isinstance(expanded, (dict, list)):
        return (
            f"<pre class=\"json-pre\">"
            f"{_json_value_html(expanded, 0)}</pre>"
        )
    text = str(expanded)
    terminal_payload = _decode_terminal_output_payload(text)
    if terminal_payload is not None:
        terminal_payload = _expand_nested_json_strings(terminal_payload)
        return (
            f"<pre class=\"json-pre\">"
            f"{_json_value_html(terminal_payload, 0)}</pre>"
        )
    parsed = _decode_json_text(text)
    if parsed is not None:
        parsed = _expand_nested_json_strings(parsed)
        return (
            f"<pre class=\"json-pre\">"
            f"{_json_value_html(parsed, 0)}</pre>"
        )
    return _audit_markdown_html(text)


def _expand_nested_json_strings(value: object) -> object:
    if isinstance(value, dict):
        return {key: _expand_nested_json_strings(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_nested_json_strings(item) for item in value]
    if isinstance(value, str):
        parsed = _decode_json_text(value)
        if parsed is not None:
            return _expand_nested_json_strings(parsed)
    return value


def _decode_json_text(text: str) -> object | None:
    stripped = text.strip()
    if not stripped or stripped[0] not in "[{":
        return None
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return None


def _decode_terminal_output_payload(text: str) -> object | None:
    marker = "Output:\n"
    if marker not in text:
        return None
    candidate = text.rsplit(marker, 1)[1].strip()
    return _decode_complete_json_text(candidate)


def _decode_complete_json_text(text: str) -> object | None:
    stripped = text.strip()
    if not stripped or stripped[0] not in "[{":
        return None
    decoder = json.JSONDecoder()
    try:
        value, end = decoder.raw_decode(stripped)
    except json.JSONDecodeError:
        return None
    if stripped[end:].strip():
        return None
    return value


def _audit_output_preview(text: str) -> str:
    expanded = _decode_terminal_output_payload(text)
    if expanded is None:
        expanded = _expand_nested_json_strings(text)
    else:
        expanded = _expand_nested_json_strings(expanded)
    if isinstance(expanded, (dict, list)):
        compact = json.dumps(expanded, ensure_ascii=False, separators=(",", ":"))
        return _excerpt(compact, 160)
    return _excerpt(str(expanded).replace("\n", " "), 160)


def _audit_markdown_html(text: str) -> str:
    blocks: list[str] = []
    list_items: list[str] = []

    def flush_list() -> None:
        if list_items:
            blocks.append("<ul>" + "".join(list_items) + "</ul>")
            list_items.clear()

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            flush_list()
            continue
        if line.startswith("- ") or line.startswith("* "):
            list_items.append(f"<li>{escape(line[2:].strip())}</li>")
            continue
        flush_list()
        if line.startswith("### "):
            blocks.append(f"<h3>{escape(line[4:].strip())}</h3>")
        elif line.startswith("## "):
            blocks.append(f"<h2>{escape(line[3:].strip())}</h2>")
        elif line.startswith("# "):
            blocks.append(f"<h1>{escape(line[2:].strip())}</h1>")
        else:
            blocks.append(f"<p>{escape(line)}</p>")
    flush_list()
    if not blocks:
        return "<div class=\"audit-tool-rendered-text\"></div>"
    return f"<div class=\"audit-tool-rendered-text\">{''.join(blocks)}</div>"


def _trigger_text(attempt: ReplyAttempt) -> str:
    if attempt.trigger_sender.strip():
        return f"{attempt.trigger_sender}: {attempt.trigger_text}"
    return attempt.trigger_text


def _json_html(text: str) -> str:
    try:
        payload = json.loads(text or "[]")
    except Exception:
        return escape(text)
    return _json_value_html(payload, 0)


def _json_value_html(value, level: int) -> str:
    indent = " " * (level * 2)
    child_indent = " " * ((level + 1) * 2)
    if isinstance(value, dict):
        if not value:
            return "{}"
        lines = ["{"]
        items = list(value.items())
        for index, (key, item_value) in enumerate(items):
            comma = "," if index < len(items) - 1 else ""
            key_html = (
                f"<span class=\"json-key\">"
                f"{escape(json.dumps(str(key), ensure_ascii=False))}</span>"
            )
            lines.append(
                f"{child_indent}{key_html}: "
                f"{_json_value_html(item_value, level + 1)}{comma}"
            )
        lines.append(f"{indent}" + "}")
        return "\n".join(lines)
    if isinstance(value, list):
        if not value:
            return "[]"
        lines = ["["]
        for index, item_value in enumerate(value):
            comma = "," if index < len(value) - 1 else ""
            lines.append(
                f"{child_indent}{_json_value_html(item_value, level + 1)}{comma}"
            )
        lines.append(f"{indent}]")
        return "\n".join(lines)
    if isinstance(value, str):
        return (
            f"<span class=\"json-string\">"
            f"{escape(json.dumps(value, ensure_ascii=False))}</span>"
        )
    if isinstance(value, bool):
        return f"<span class=\"json-bool\">{str(value).lower()}</span>"
    if value is None:
        return "<span class=\"json-null\">null</span>"
    return f"<span class=\"json-number\">{escape(str(value))}</span>"


def _attempt_id_from_path(path: str) -> int | None:
    parts = path.strip("/").split("/")
    if len(parts) != 2 or parts[0] != "attempts":
        return None
    try:
        return int(parts[1])
    except ValueError:
        return None


def _excerpt(text: str, length: int) -> str:
    normalized = " ".join(text.split())
    if len(normalized) <= length:
        return normalized
    return f"{normalized[:length]}..."
