"""Dagster report assets."""

import logging
import sys
from pathlib import Path

from dagster import AssetExecutionContext, MetadataValue, Output, asset

logger = logging.getLogger(__name__)

# Ensure the reports package is importable (sibling directory at project root)
_REPORTS_DIR = Path(__file__).parent.parent.parent / "reports"
if str(_REPORTS_DIR.parent) not in sys.path:
    sys.path.insert(0, str(_REPORTS_DIR.parent))

OUTPUT_DIR = Path(__file__).parent.parent.parent / "reports" / "output"


@asset(
    group_name="reports",
    compute_kind="python",
    description="Jupyter notebook rendered to HTML — interactive energy analytics for executive presentation.",
    deps=["gold_generation_by_fuel_monthly", "gold_price_trends", "gold_carbon_footprint", "gold_executive_summary"],
)
def jupyter_html_report(context: AssetExecutionContext) -> Output:
    """Generate and render the Jupyter analytics notebook to HTML."""
    from reports.generators.jupyter_report import JupyterReportGenerator

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "energy_analytics_report.html"

    context.log.info("Generating Jupyter HTML report → %s", output_path)
    generator = JupyterReportGenerator()
    generator.generate(str(output_path))

    file_size = output_path.stat().st_size
    context.log.info("Jupyter HTML report generated: %.1f KB", file_size / 1024)

    return Output(
        value=str(output_path),
        metadata={
            "path":       MetadataValue.path(str(output_path)),
            "size_bytes": MetadataValue.int(file_size),
            "format":     MetadataValue.text("HTML (from Jupyter Notebook)"),
        },
    )
