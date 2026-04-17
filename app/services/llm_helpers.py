import json
import httpx

from web_config.llm_config import (
    OLLAMA_BASE_URL,
    OLLAMA_MODEL,
    OLLAMA_THINK,
    OLLAMA_TIMEOUT_STREAM,
    OLLAMA_TIMEOUT_SYNC,
)


def request_ollama_insights_text(prompt, model=None):
    """Ollamaに同期リクエストを送信し、生成テキストを返す。"""
    resolved_model = model or OLLAMA_MODEL
    request_body = {
        "model": resolved_model,
        "prompt": prompt,
        "stream": False,
    }
    if not OLLAMA_THINK:
        request_body["think"] = False

    with httpx.Client(timeout=OLLAMA_TIMEOUT_SYNC) as client:
        response = client.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json=request_body,
        )
        response.raise_for_status()
        payload = response.json()
        return str(payload.get("response") or "").strip()


async def stream_ollama_response(prompt, model=None, httpx_module=None):
    """Ollamaに非同期ストリーミングリクエストを送信し、トークンを逐次yieldする。

    Yields:
        dict: {"token": str} または {"done": True} または {"error": str}
    """
    resolved_model = model or OLLAMA_MODEL
    resolved_httpx = httpx_module or httpx
    request_body = {
        "model": resolved_model,
        "prompt": prompt,
        "stream": True,
    }
    if not OLLAMA_THINK:
        request_body["think"] = False

    try:
        async with resolved_httpx.AsyncClient(timeout=OLLAMA_TIMEOUT_STREAM) as client:
            async with client.stream(
                "POST",
                f"{OLLAMA_BASE_URL}/api/generate",
                json=request_body,
            ) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    chunk = json.loads(line)
                    token = chunk.get("response", "")
                    if token:
                        yield {"token": token}
                    if chunk.get("done"):
                        yield {"done": True}
                        return
    except resolved_httpx.ConnectError:
        yield {"error": "Ollamaが起動していません"}
    except Exception as exc:
        msg = str(exc) or f"{type(exc).__name__}: 詳細不明"
        yield {"error": msg}


def build_bottleneck_prompt(data: dict) -> str:
    freq = data.get("frequency_top10", [])
    slow = sorted(
        freq,
        key=lambda x: x.get("平均処理時間_分", x.get("平均処理時間", 0)),
        reverse=True,
    )[:3]
    busy = sorted(freq, key=lambda x: x.get("イベント件数", 0), reverse=True)[:3]
    patterns = data.get("pattern_top10", [])[:3]

    return f"""あなたはプロセス改善の業務分析者です。
以下のプロセスマイニング分析結果をもとに、現場担当者が今日から実行できるレベルの解説をしてください。
数値の羅列ではなく、なぜそうなっているかの仮説を含めてください。

## 分析データ
- ケース数: {data.get("total_cases", "不明")}
- 分析期間: {data.get("period", "不明")}
- 処理時間が長いアクティビティ上位3: {slow}
- 件数が集中しているアクティビティ上位3: {busy}
- 主要プロセスパターン上位3: {patterns}

## 出力形式（この5セクションで出力してください）

1. 全体サマリー
このプロセス全体の状況を2〜3文で要約してください。

2. ボトルネックの特徴
最も問題が強そうな箇所と考えられる理由を、使った数値を交えて説明してください。

3. 考えられる次原因
なぜそこがボトルネックになっているか、現場でよくある原因を3つ挙げてください。

4. 改善アクション
明日から実行できる具体的なアクションを3つ、優先順位付きで提案してください。
1つ目は「すぐできること（工数小）」、2つ目は「中期的な改善（工数大・効果大）」として記載してください。

5. 次のステップ
改善を進める上で、次に追加で分析すべきことを1つ提案してください。

出力は現場担当者にも伝わるような自然な日本語で、専門用語は必要最低限にしてください。
マークダウン記法（**、##、- のリスト記号など）は使わず、プレーンテキストで出力してください。
強調したい箇所は「」で囲んでください。
"""
