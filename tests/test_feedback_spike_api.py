from pathlib import Path


API_DIR = Path(__file__).resolve().parents[1] / "api"


def test_callback_endpoint_accepts_get_and_post_and_redacts_headers():
    source = (API_DIR / "dingtalk-feedback-spike.js").read_text(encoding="utf-8")

    assert '["GET", "POST"].includes(req.method)' in source
    assert 'import { persistFeedbackEvent } from "./feedback-storage.js";' in source
    assert '"feedback_token"' in source
    assert '"feedbackToken"' in source
    assert '"attempt_id"' in source
    assert '"attemptId"' in source
    assert '"rating"' in source
    assert '"original_text"' in source
    assert '"reply_text"' in source
    assert '"comment"' in source
    assert '"suggested_reply"' in source
    assert "safeHeaders" in source
    assert "content-type" in source
    safe_headers_source = source.split("function safeHeaders", 1)[1].split(
        "function extractBody", 1
    )[0]
    assert "authorization" not in safe_headers_source.lower()


def test_callback_endpoint_writes_event_list_and_expiring_event_key():
    source = (API_DIR / "dingtalk-feedback-spike.js").read_text(encoding="utf-8")
    storage_source = (API_DIR / "feedback-storage.js").read_text(encoding="utf-8")

    assert 'export const EVENT_LIST_KEY = "feedback-spike-events"' in storage_source
    assert 'const EVENT_KEY_PREFIX = "feedback-spike:"' in source
    assert 'from "@vercel/blob";' in storage_source
    assert 'from "@tigrisdata/storage";' in storage_source
    assert 'await putJson(`${EVENT_LIST_KEY}/${event.key}.json`, payload)' in storage_source
    assert '`${EVENT_LIST_KEY}/by-token/${tokenPathSegment(event.feedback_token)}/${event.key}.json`' in storage_source
    assert 'access: "private"' in storage_source
    assert "blobGet(path, { access: \"private\" })" in storage_source
    assert "BLOB_READ_WRITE_TOKEN" in storage_source
    assert "blob_not_configured:BLOB_READ_WRITE_TOKEN" in storage_source
    assert "storage_backend" in source
    assert "TIGRIS_STORAGE_ACCESS_KEY_ID" in storage_source
    assert "TIGRIS_STORAGE_SECRET_ACCESS_KEY" in storage_source
    assert "TIGRIS_STORAGE_BUCKET" in storage_source
    assert "提交遇到问题" in source
    assert "ok: persisted" in source


def test_callback_endpoint_renders_simple_user_feedback_page():
    source = (API_DIR / "dingtalk-feedback-spike.js").read_text(encoding="utf-8")

    assert "renderFeedbackPage" in source
    assert "renderSubmittedPage" in source
    assert "这条回复有帮助吗？" in source
    assert "你的反馈会帮助改进自动回复质量。" in source
    assert "friday-logo.svg" in source
    assert "原话" in source
    assert "回复样例" in source
    assert "Attempt #" in source
    assert 'name="attempt_id"' in source
    assert "评语（可选）" in source
    assert "可以补充哪里没答好、哪里有帮助。" in source
    assert "快捷反馈" not in source
    assert "判断不准确" not in source
    assert "语气不合适" not in source
    assert "缺少信息" not in source
    assert "renderRatingOptions(context.rating)" in source
    assert "特别没用" in source
    assert "不太有用" in source
    assert "一般" in source
    assert "很有用" in source
    assert "非常有用" in source
    assert 'name="suggested_reply"' not in source
    assert 'up: "useful"' in source
    assert 'down: "not_useful"' in source
    assert 'method="post"' in source
    assert "application/json" in source
    assert "URLSearchParams" in source


def test_events_endpoint_requires_secret_and_reads_recent_events():
    source = (API_DIR / "dingtalk-feedback-spike-events.js").read_text(encoding="utf-8")
    storage_source = (API_DIR / "feedback-storage.js").read_text(encoding="utf-8")

    assert 'from "@vercel/blob";' in storage_source
    assert 'from "@tigrisdata/storage";' in storage_source
    assert "readFeedbackEvent" in source
    assert "FEEDBACK_SPIKE_SECRET" in source
    assert "x-feedback-spike-secret" in source
    assert "requestFeedbackToken" in source
    assert "tokenPathSegment" in source
    assert "listFeedbackEventPaths" in source
    assert '"feedback_token"' in source
    assert "filteredEvents" in source
    assert 'res.status(401).json({ ok: false, error: "unauthorized" })' in source
    assert "BLOB_READ_WRITE_TOKEN" in storage_source
    assert "storageConfigError" in source
    assert '`${EVENT_LIST_KEY}/by-token/${tokenPathSegment(feedbackToken)}/`' in source
    assert "path.includes(\"/by-token/\")" in source
