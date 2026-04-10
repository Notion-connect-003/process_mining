import httpx


def request_ollama_insights_text(prompt, model="qwen2.5:7b"):
    timeout = httpx.Timeout(connect=5.0, read=60.0, write=10.0, pool=5.0)
    with httpx.Client(timeout=timeout) as client:
        response = client.post(
            "http://localhost:11434/api/generate",
            json={"model": model, "prompt": prompt, "stream": False},
        )
        response.raise_for_status()
        payload = response.json()
        return str(payload.get("response") or "").strip()


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
"""
