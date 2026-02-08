import os
import time
import signal
import sys
from dataclasses import dataclass

import requests
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter


@dataclass(frozen=True)
class Config:
    target_url: str
    interval_s: float
    timeout_s: float
    threshold_ms: float
    host_header: str

    otlp_endpoint: str
    otlp_insecure: bool
    service_name: str
    export_interval_ms: int


def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def load_config() -> Config:
    return Config(
        target_url=os.getenv("TARGET_URL", "http://127.0.0.1:8000/graphql/"),
        interval_s=float(os.getenv("INTERVAL_SECONDS", "1")),
        timeout_s=float(os.getenv("TIMEOUT_SECONDS", "5")),
        threshold_ms=float(os.getenv("THRESHOLD_MS", "100")),
        host_header=os.getenv("HOST_HEADER", "").strip(),
        otlp_endpoint=os.getenv(
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "http://otel-collector.monitoring.svc.cluster.local:4317",
        ).strip(),
        otlp_insecure=env_bool("OTEL_EXPORTER_OTLP_INSECURE", True),
        service_name=os.getenv("OTEL_SERVICE_NAME", "saleor-synth-probe").strip(),
        export_interval_ms=int(os.getenv("OTEL_METRIC_EXPORT_INTERVAL_MS", "5000")),
    )


def build_meter(cfg: Config):
    resource = Resource.create({"service.name": cfg.service_name})
    exporter = OTLPMetricExporter(endpoint=cfg.otlp_endpoint, insecure=cfg.otlp_insecure)
    reader = PeriodicExportingMetricReader(
        exporter, export_interval_millis=cfg.export_interval_ms
    )
    provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(provider)
    return metrics.get_meter("saleor-synth-probe")


class Probe:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.session = requests.Session()

        meter = build_meter(cfg)
        # Histograms
        self.h_ttfb = meter.create_histogram("saleor.synthetic.http.ttfb_ms", unit="ms")
        self.h_total = meter.create_histogram("saleor.synthetic.http.total_ms", unit="ms")
        # Counters
        self.c_over = meter.create_counter("saleor.synthetic.http.over_threshold_total")
        self.c_fail = meter.create_counter("saleor.synthetic.http.fail_total")

        self._stop = False

    def stop(self):
        self._stop = True

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.cfg.host_header:
            headers["Host"] = self.cfg.host_header
        return headers

    def run_once(self) -> None:
        # Cheap GraphQL that should be fast and mostly app-path (change if you want DB included)
        payload = {"query": "query { shop { __typename } }"}

        start = time.monotonic()
        status_code = "error"

        try:
            r = self.session.post(
                self.cfg.target_url,
                headers=self._headers(),
                json=payload,
                timeout=self.cfg.timeout_s,
                stream=True,
            )
            ttfb_ms = (time.monotonic() - start) * 1000.0

            # force body read for total
            _ = r.content
            total_ms = (time.monotonic() - start) * 1000.0

            status_code = str(r.status_code)
            attrs = {"http.status_code": status_code}

            self.h_ttfb.record(ttfb_ms, attrs)
            self.h_total.record(total_ms, attrs)

            if total_ms > self.cfg.threshold_ms:
                self.c_over.add(
                    1, {"threshold_ms": str(int(self.cfg.threshold_ms)), "http.status_code": status_code}
                )

            if r.status_code >= 500:
                self.c_fail.add(1, {"reason": "5xx", "http.status_code": status_code})

        except requests.Timeout:
            total_ms = (time.monotonic() - start) * 1000.0
            self.h_total.record(total_ms, {"http.status_code": "timeout"})
            self.c_fail.add(1, {"reason": "timeout", "http.status_code": "timeout"})

        except Exception:
            total_ms = (time.monotonic() - start) * 1000.0
            self.h_total.record(total_ms, {"http.status_code": status_code})
            self.c_fail.add(1, {"reason": "exception", "http.status_code": status_code})

    def loop(self) -> None:
        while not self._stop:
            t0 = time.monotonic()
            self.run_once()
            # keep cadence stable-ish
            sleep_for = max(0.0, self.cfg.interval_s - (time.monotonic() - t0))
            time.sleep(sleep_for)


def main() -> None:
    cfg = load_config()
    probe = Probe(cfg)

    def _handle_sig(_signum, _frame):
        probe.stop()

    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    probe.loop()
    sys.exit(0)