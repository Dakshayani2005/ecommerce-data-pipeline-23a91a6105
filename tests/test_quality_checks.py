import json
import os

REPORT_PATH = "data/processed/monitoring_report.json"

def test_quality_report_exists():
    assert os.path.exists(REPORT_PATH)

def test_quality_score_valid():
    with open(REPORT_PATH) as f:
        report = json.load(f)

    assert "checks" in report
    assert "data_quality" in report["checks"]
    assert 0 <= report["checks"]["data_quality"]["quality_score"] <= 100

 
