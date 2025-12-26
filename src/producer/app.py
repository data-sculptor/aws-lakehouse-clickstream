import argparse
import json
import random
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from faker import Faker

fake = Faker()

EVENT_TYPES = ["page_view", "add_to_cart", "purchase"]
PAGES = ["/", "/search", "/product", "/cart", "/checkout", "/help", "/pricing"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
OSES = ["iOS", "Android", "Windows", "macOS", "Linux"]
COUNTRIES = ["DE", "FR", "NL", "GB", "US", "PL", "SE", "ES", "IT"]


@dataclass
class ClickstreamEvent:
    event_id: str
    event_ts: str
    user_id: str
    session_id: str
    event_type: str
    page: str
    referrer: str
    device: Dict[str, str]
    geo: Dict[str, str]
    attributes: Dict[str, Any]


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def new_user_id() -> str:
    # keep it stable-ish and readable: "usr_<uuid4>"
    return f"usr_{uuid.uuid4().hex[:16]}"


def new_session_id() -> str:
    return f"sess_{uuid.uuid4().hex[:16]}"


def make_event(user_id: str, session_id: str, event_type: Optional[str] = None) -> ClickstreamEvent:
    etype = event_type or random.choice(EVENT_TYPES)

    page = random.choice(PAGES)
    if etype in ("add_to_cart", "purchase"):
        page = random.choice(["/product", "/cart", "/checkout"])

    referrer = random.choice(
        ["direct", "google", "newsletter", "twitter", "linkedin", "partner_site"]
    )

    device = {
        "os": random.choice(OSES),
        "browser": random.choice(BROWSERS),
    }

    country = random.choice(COUNTRIES)
    geo = {
        "country": country,
        "city": fake.city(),
    }

    attrs: Dict[str, Any] = {
        "ab_test_variant": random.choice(["A", "B"]),
        "utm_campaign": random.choice(["brand", "summer_sale", "retargeting", "none"]),
    }

    # add commerce-ish fields for some events
    if etype in ("add_to_cart", "purchase"):
        attrs.update(
            {
                "product_id": f"sku_{random.randint(1000, 9999)}",
                "price": round(random.uniform(5.0, 300.0), 2),
                "currency": random.choice(["EUR", "USD", "GBP"]),
                "quantity": random.randint(1, 3),
            }
        )

    if etype == "purchase":
        attrs.update(
            {
                "order_id": f"ord_{uuid.uuid4().hex[:12]}",
                "payment_method": random.choice(["card", "paypal", "klarna"]),
            }
        )

    return ClickstreamEvent(
        event_id=str(uuid.uuid4()),
        event_ts=iso_utc_now(),
        user_id=user_id,
        session_id=session_id,
        event_type=etype,
        page=page,
        referrer=referrer,
        device=device,
        geo=geo,
        attributes=attrs,
    )


def generate_session_events(user_id: str, max_events: int) -> List[ClickstreamEvent]:
    """
    Make a realistic session:
    - always starts with page_view
    - may add_to_cart
    - may purchase (lower probability)
    """
    session_id = new_session_id()
    events: List[ClickstreamEvent] = [make_event(user_id, session_id, "page_view")]

    n = random.randint(1, max_events)
    for _ in range(n - 1):
        # bias towards page_view
        etype = random.choices(
            population=["page_view", "add_to_cart", "purchase"],
            weights=[0.78, 0.18, 0.04],
            k=1,
        )[0]
        events.append(make_event(user_id, session_id, etype))

    return events


def main() -> None:
    
    parser = argparse.ArgumentParser(description="Generate clickstream events (JSONL).")
    parser.add_argument("--events", type=int, default=200, help="Total events to emit")
    parser.add_argument("--max-events-per-session", type=int, default=12, help="Max events per session")
    parser.add_argument("--sleep-ms", type=int, default=0, help="Sleep between events (simulate streaming)")
    parser.add_argument(
        "--out",
        type=str,
        default="-",
        help="Output path for JSONL. Use '-' for stdout (default).",
    )
    parser.add_argument(
        "--dup-rate",
        type=float,
        default=0.01,
        help="Probability [0..1] to emit a duplicate event_id (to test dedupe in Silver).",
    )
    parser.add_argument(
        "--oo-rate",
        type=float,
        default=0.02,
        help="Probability [0..1] to emit an event with an older timestamp (to test out-of-order handling).",
    )
    args = parser.parse_args()

    print("producer starting with args:", args)

    # Keep some state so we can intentionally emit duplicates
    previously_emitted: List[Dict[str, Any]] = []

    out_fh = None
    try:
        if args.out == "-":
            out_fh = None
        else:
            # Ensure folder exists (simple approach)
            import os
            os.makedirs(os.path.dirname(args.out), exist_ok=True)
            out_fh = open(args.out, "w", encoding="utf-8")

        emitted = 0
        while emitted < args.events:
            user_id = new_user_id()
            session_events = generate_session_events(user_id, args.max_events_per_session)

            for ev in session_events:
                if emitted >= args.events:
                    break

                payload = asdict(ev)

                # Inject out-of-order timestamps sometimes
                if random.random() < args.oo_rate:
                    older = datetime.now(timezone.utc).timestamp() - random.randint(60, 3600)
                    payload["event_ts"] = datetime.fromtimestamp(older, tz=timezone.utc).isoformat(timespec="milliseconds")

                # Inject duplicates sometimes
                if previously_emitted and random.random() < args.dup_rate:
                    payload = random.choice(previously_emitted)

                line = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

                if out_fh:
                    out_fh.write(line + "\n")
                else:
                    print(line)

                # Keep a sample for possible duplicates
                previously_emitted.append(payload)
                if len(previously_emitted) > 5000:
                    previously_emitted = previously_emitted[-2000:]

                emitted += 1

                if args.sleep_ms > 0:
                    time.sleep(args.sleep_ms / 1000.0)

    finally:
        if out_fh:
            out_fh.close()


if __name__ == "__main__":
    main()
