from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_feedback_page_posts_to_real_feedback_api():
    source = (ROOT / "index.html").read_text(encoding="utf-8")

    assert "/api/dingtalk-feedback-spike?format=json" in source
    assert "/api/dingtalk-feedback-spike-events?feedback_token=" not in source
    assert "friday-feedback-page" in source
    assert "feedback_token: token" in source
    assert "friday-logo.svg" in source
    assert "这条回复有帮助吗？" in source
    assert "你的反馈会帮助改进自动回复质量。" in source
    assert "Attempt #" in source
    assert "原话" in source
    assert "回复样例" in source
    assert "评语（可选）" in source
    assert "可以补充哪里没答好、哪里有帮助。" in source
    assert "快捷反馈" not in source
    assert "判断不准确" not in source
    assert "name=\"suggested_reply\"" not in source
    assert 'up: "useful"' in source
    assert 'down: "not_useful"' in source
    assert 'value="useful" checked' in source
    assert "<span>很有用</span>" in source
    assert "<span>不太有用</span>" in source
    assert "very_unhelpful" in source
    assert "very_useful" in source
    assert "查看 JSON" not in source
    assert "打开 API 表单" not in source
    assert "当前 token" not in source
    assert "纯静态" not in source
    assert "已收到，谢谢。" in source
