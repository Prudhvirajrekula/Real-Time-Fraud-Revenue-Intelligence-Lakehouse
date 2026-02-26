"""Device fingerprinting event generator."""

import random
from typing import Any

from .base import (
    BaseGenerator,
    BROWSERS,
    BROWSER_WEIGHTS,
    DEVICE_TYPES,
    DEVICE_TYPE_WEIGHTS,
    OS_BY_DEVICE,
    fake,
)

SCREEN_RESOLUTIONS = ["1920x1080", "1366x768", "1440x900", "2560x1440", "390x844", "375x812", "1280x800"]
TIMEZONES = ["America/New_York", "America/Los_Angeles", "Europe/London", "Europe/Berlin",
             "Asia/Tokyo", "Asia/Kolkata", "Australia/Sydney", "America/Sao_Paulo"]
LANGUAGES = ["en-US", "en-GB", "de-DE", "fr-FR", "ja-JP", "pt-BR", "zh-CN", "hi-IN"]


class DeviceGenerator(BaseGenerator):

    def generate_for_user(self, user: dict, session_id: str, is_fraud: bool = False) -> dict[str, Any]:
        device_type = random.choices(DEVICE_TYPES, weights=DEVICE_TYPE_WEIGHTS)[0]
        os_info = random.choice(OS_BY_DEVICE[device_type])
        browser = random.choices(BROWSERS, weights=BROWSER_WEIGHTS)[0]
        browser_version = f"{random.randint(80, 120)}.0.{random.randint(1000, 9999)}.{random.randint(10, 99)}"
        now = self.now_ms()

        # Fraud devices often use VPN/Proxy/Tor
        vpn = (random.random() < 0.60) if is_fraud else (random.random() < 0.03)
        proxy = (random.random() < 0.30) if is_fraud else (random.random() < 0.01)
        tor = (random.random() < 0.10) if is_fraud else (random.random() < 0.002)
        bot = (random.random() < 0.05) if is_fraud else (random.random() < 0.001)

        return {
            "device_id": self.new_id(),
            "user_id": user["user_id"],
            "session_id": session_id,
            "device_type": device_type,
            "os": os_info[0],
            "os_version": os_info[1],
            "browser": browser,
            "browser_version": browser_version,
            "fingerprint_hash": self.fingerprint(user["user_id"], device_type, os_info[0], browser),
            "ip_address": self.random_ip(high_risk=is_fraud),
            "vpn_detected": vpn,
            "proxy_detected": proxy,
            "tor_detected": tor,
            "bot_detected": bot,
            "screen_resolution": random.choice(SCREEN_RESOLUTIONS),
            "timezone": random.choice(TIMEZONES),
            "language": random.choice(LANGUAGES),
            "created_at": now,
            "ingest_timestamp": now,
        }
