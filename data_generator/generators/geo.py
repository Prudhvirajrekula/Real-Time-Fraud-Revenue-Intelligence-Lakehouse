"""Geo-location event generator with city/country/coordinate data."""

import random
from typing import Any

from .base import BaseGenerator, COUNTRY_WEIGHTS, HIGH_RISK_COUNTRIES, fake

# Country → approximate lat/lon center + bounding box
COUNTRY_GEO = {
    "US": {"lat": (25.0, 49.0), "lon": (-125.0, -67.0), "tz": "America/New_York"},
    "GB": {"lat": (50.0, 59.0), "lon": (-8.0, 2.0),  "tz": "Europe/London"},
    "DE": {"lat": (47.0, 55.0), "lon": (6.0, 15.0),  "tz": "Europe/Berlin"},
    "FR": {"lat": (43.0, 51.0), "lon": (-5.0, 8.0),  "tz": "Europe/Paris"},
    "CA": {"lat": (43.0, 60.0), "lon": (-140.0, -52.0), "tz": "America/Toronto"},
    "AU": {"lat": (-43.0, -10.0), "lon": (113.0, 153.0), "tz": "Australia/Sydney"},
    "JP": {"lat": (30.0, 45.0), "lon": (130.0, 145.0), "tz": "Asia/Tokyo"},
    "BR": {"lat": (-33.0, 5.0), "lon": (-74.0, -34.0), "tz": "America/Sao_Paulo"},
    "IN": {"lat": (8.0, 37.0),  "lon": (68.0, 97.0),  "tz": "Asia/Kolkata"},
    "NG": {"lat": (4.0, 14.0),  "lon": (3.0, 15.0),  "tz": "Africa/Lagos"},
    "RU": {"lat": (41.0, 71.0), "lon": (27.0, 180.0), "tz": "Europe/Moscow"},
    "CN": {"lat": (18.0, 53.0), "lon": (73.0, 135.0), "tz": "Asia/Shanghai"},
    "MX": {"lat": (14.0, 32.0), "lon": (-118.0, -87.0), "tz": "America/Mexico_City"},
    "ZA": {"lat": (-35.0, -22.0), "lon": (16.0, 33.0), "tz": "Africa/Johannesburg"},
    "PH": {"lat": (5.0, 20.0),  "lon": (117.0, 126.0), "tz": "Asia/Manila"},
}

ISPs = [
    "Comcast Cable", "AT&T Internet", "Verizon Fios", "Deutsche Telekom",
    "BT Group", "Orange", "NTT", "Vodafone", "Amazon AWS", "Google Cloud",
    "Microsoft Azure", "DigitalOcean", "Linode", "OVH", "Hetzner",
]


class GeoEventGenerator(BaseGenerator):

    def generate_for_session(
        self,
        user: dict,
        session_id: str,
        event_type: str = "order",
        is_fraud: bool = False
    ) -> dict[str, Any]:
        country = user["country"]
        geo = COUNTRY_GEO.get(country, COUNTRY_GEO["US"])
        now = self.now_ms()

        # Fraud events more likely to use VPN/datacenter IP
        is_vpn = (random.random() < 0.55) if is_fraud else (random.random() < 0.03)
        is_proxy = (random.random() < 0.25) if is_fraud else (random.random() < 0.01)
        is_datacenter = is_vpn or is_proxy

        lat = round(random.uniform(*geo["lat"]), 6)
        lon = round(random.uniform(*geo["lon"]), 6)
        isp = random.choice(ISPs)
        if is_datacenter:
            isp = random.choice(["Amazon AWS", "Google Cloud", "Microsoft Azure", "DigitalOcean", "Hetzner"])

        return {
            "event_id": self.new_id(),
            "user_id": user["user_id"],
            "session_id": session_id,
            "ip_address": self.random_ip(high_risk=is_fraud),
            "country_code": country,
            "country_name": fake.country(),
            "city": fake.city(),
            "region": fake.state() if country == "US" else fake.city(),
            "latitude": lat,
            "longitude": lon,
            "timezone": geo["tz"],
            "isp": isp,
            "is_vpn": is_vpn,
            "is_proxy": is_proxy,
            "is_datacenter": is_datacenter,
            "event_type": event_type,
            "created_at": now,
            "ingest_timestamp": now,
        }
