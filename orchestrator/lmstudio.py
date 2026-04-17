from __future__ import annotations

import json
import re
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

try:
    import requests
except ModuleNotFoundError:  # pragma: no cover - optional dependency fallback
    requests = None

from .config import AppConfig
from .models import Classification


JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)


@dataclass(slots=True)
class LmStudioClient:
    config: AppConfig

    def classify_book(
        self,
        filename: str,
        excerpt: str,
        allowed_genres: list[str],
        deep: bool = False,
    ) -> Classification:
        lm = self.config.lmstudio
        model = lm.deep_model if deep else lm.fast_model
        excerpt_word_limit = lm.deep_excerpt_words if deep else lm.fast_excerpt_words
        max_input_tokens = lm.deep_max_input_tokens if deep else lm.fast_max_input_tokens
        max_output_tokens = lm.deep_max_output_tokens if deep else lm.fast_max_output_tokens
        excerpt = " ".join(excerpt.split()[:excerpt_word_limit]).strip()
        excerpt = self._trim_excerpt_to_token_budget(
            filename=filename,
            excerpt=excerpt,
            allowed_genres=allowed_genres,
            deep=deep,
            max_input_tokens=max_input_tokens,
        )
        prompt = self._build_prompt(filename, excerpt, allowed_genres, deep=deep)
        payload = {
            "model": model,
            "temperature": lm.temperature,
            "max_tokens": max_output_tokens,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You classify books for a Russian library catalog. "
                        "Return only compact JSON with no extra text."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        }
        content = self._post_chat_completion(payload)
        payload = self._extract_json(content)
        return Classification(
            author=self._text_value(payload.get("author")),
            title=self._text_value(payload.get("title")),
            genre=self._text_value(payload.get("genre"), "Не распознано") or "Не распознано",
            confidence=self._float_value(payload.get("confidence")),
            reasoning=self._text_value(payload.get("reasoning")),
            needs_deep_analysis=bool(payload.get("needs_deep_analysis", False)),
        )

    def _build_prompt(
        self,
        filename: str,
        excerpt: str,
        allowed_genres: list[str],
        *,
        deep: bool,
    ) -> str:
        mode = "deep" if deep else "fast"
        return (
            f"Mode: {mode}\n"
            "Analyze the filename and a short text excerpt.\n"
            f"Filename: {filename}\n"
            f"Allowed genres: {', '.join(allowed_genres)}\n"
            f"Excerpt:\n{excerpt or '[no excerpt]'}\n\n"
            "Return only one compact JSON object with keys:\n"
            "author, title, genre, confidence, reasoning, needs_deep_analysis.\n"
            "Rules:\n"
            "- Keep reasoning under 12 words.\n"
            "- Do not add markdown.\n"
            "- Do not explain your answer.\n"
            "- Confidence must be from 0.0 to 1.0.\n"
            "- If unsure, set needs_deep_analysis to true."
        )

    def _extract_json(self, content: str) -> dict:
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            match = JSON_BLOCK_RE.search(content)
            if not match:
                return {}
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                return {}

    def _post_chat_completion(self, payload: dict) -> str:
        url = f"{self.config.lmstudio.base_url.rstrip('/')}/chat/completions"
        if requests is not None:
            response = requests.post(url, timeout=self.config.lmstudio.timeout_seconds, json=payload)
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]

        request = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.config.lmstudio.timeout_seconds) as response:
                raw = response.read().decode("utf-8")
        except (HTTPError, URLError) as error:  # pragma: no cover - network runtime path
            raise RuntimeError(f"LM Studio request failed: {error}") from error
        body = json.loads(raw)
        return body["choices"][0]["message"]["content"]

    def _trim_excerpt_to_token_budget(
        self,
        filename: str,
        excerpt: str,
        allowed_genres: list[str],
        *,
        deep: bool,
        max_input_tokens: int,
    ) -> str:
        if not excerpt:
            return excerpt
        prompt_without_excerpt = self._build_prompt(filename, "", allowed_genres, deep=deep)
        reserved_chars = len(prompt_without_excerpt) + 256
        char_budget = max((max_input_tokens * 4) - reserved_chars, 256)
        if len(excerpt) <= char_budget:
            return excerpt
        trimmed = excerpt[:char_budget]
        if " " in trimmed:
            trimmed = trimmed.rsplit(" ", 1)[0]
        return trimmed.strip()

    def _text_value(self, value, default: str = "") -> str:
        if value is None:
            return default
        if not isinstance(value, str):
            value = str(value)
        return value.strip()

    def _float_value(self, value) -> float:
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0
