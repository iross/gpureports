#!/usr/bin/env python3
"""
GPU Usage Statistics - Reporting Functions

HTML report generation, text output, email sending, and
formatted display of analysis results.
"""

import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from device_name_mappings import get_human_readable_device_name
from gpu_utils import (
    CLASS_ORDER,
    get_display_name,
    get_gpu_performance_tier,
)


def print_gpu_model_analysis(analysis: dict):
    """Print GPU model analysis results in a formatted way."""
    if "error" in analysis:
        print(analysis["error"])
        return

    gpu_model = analysis["gpu_model"]
    snapshot_time = analysis["snapshot_time"]
    target_time = analysis["target_time"]
    summary = analysis["summary"]
    by_class = analysis["by_class"]
    machines = analysis["machines"]
    active_jobs = analysis["active_jobs"]
    inactive_gpus = analysis["inactive_gpus"]

    print(f"\n{'=' * 80}")
    print(f"GPU MODEL ACTIVITY REPORT: {gpu_model}")
    print(f"{'=' * 80}")
    print(f"Target Time: {target_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Snapshot Time: {snapshot_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Time Difference: {abs((snapshot_time - target_time).total_seconds())} seconds")

    print("\nSUMMARY:")
    print(f"{'-' * 40}")
    print(f"Total GPUs: {summary['total_gpus']}")
    print(f"Active (with jobs): {summary['claimed_gpus']} ({summary['utilization_percent']:.1f}%)")
    print(f"Idle (no jobs): {summary['unclaimed_gpus']}")
    print(f"Avg GPU Usage: {summary['avg_gpu_usage_percent']:.1f}%")
    print(f"Machines: {summary['num_machines']}")

    # Separate real slots (Priority + Shared) from backfill slots
    real_slot_classes = ["Priority", "Shared"]
    backfill_slot_classes = ["Backfill"]

    # Calculate totals for real slots
    real_total = sum(by_class[class_name]["total"] for class_name in real_slot_classes if class_name in by_class)
    real_claimed = sum(by_class[class_name]["claimed"] for class_name in real_slot_classes if class_name in by_class)
    real_usage_pct = (real_claimed / real_total * 100) if real_total > 0 else 0

    # Calculate totals for backfill slots
    backfill_total = sum(
        by_class[class_name]["total"] for class_name in backfill_slot_classes if class_name in by_class
    )
    backfill_claimed = sum(
        by_class[class_name]["claimed"] for class_name in backfill_slot_classes if class_name in by_class
    )
    backfill_usage_pct = (backfill_claimed / backfill_total * 100) if backfill_total > 0 else 0

    print("\nREAL SLOTS:")
    print(f"{'-' * 40}")
    print(f"  TOTAL: {real_claimed}/{real_total} ({real_usage_pct:.1f}%)")
    print(f"{'-' * 40}")
    for class_name in real_slot_classes:
        if class_name in by_class and by_class[class_name]["total"] > 0:
            stats = by_class[class_name]
            usage_pct = (stats["claimed"] / stats["total"] * 100) if stats["total"] > 0 else 0
            print(f"  {class_name}: {stats['claimed']}/{stats['total']} ({usage_pct:.1f}%)")

    print("\nBACKFILL SLOTS:")
    print(f"{'-' * 40}")
    print(f"  TOTAL: {backfill_claimed}/{backfill_total} ({backfill_usage_pct:.1f}%)")
    print(f"{'-' * 40}")
    for class_name in backfill_slot_classes:
        if class_name in by_class and by_class[class_name]["total"] > 0:
            stats = by_class[class_name]
            usage_pct = (stats["claimed"] / stats["total"] * 100) if stats["total"] > 0 else 0
            print(f"  {class_name}: {stats['claimed']}/{stats['total']} ({usage_pct:.1f}%)")

    print(f"\nMACHINES ({len(machines)}):")
    print(f"{'-' * 40}")
    for machine in sorted(machines):
        print(f"  {machine}")

    if active_jobs:
        print(f"\nACTIVE JOBS ({len(active_jobs)}):")
        print(f"{'-' * 60}")
        print("  User                | Job ID          | GPU ID      | Machine")
        print(f"{'-' * 60}")
        for job in active_jobs:
            user = (job.get("RemoteOwner") or "N/A")[:19]
            job_id = (job.get("GlobalJobId") or "N/A")[:14]
            gpu_id = (job.get("AssignedGPUs") or "N/A")[:11]
            machine = (job.get("Machine") or "N/A")[:19]
            print(f"  {user:<18} | {job_id:<15} | {gpu_id:<11} | {machine}")
    else:
        print("\nNo active jobs found.")

    if inactive_gpus:
        print(f"\nINACTIVE GPUs ({len(inactive_gpus)}):")
        print(f"{'-' * 60}")
        print("  GPU ID      | Machine             | Priority Projects")
        print(f"{'-' * 60}")
        for gpu in inactive_gpus:
            gpu_id = (gpu.get("AssignedGPUs") or "N/A")[:11]
            machine = (gpu.get("Machine") or "N/A")[:19]
            priority_projects = (gpu.get("PrioritizedProjects") or "None")[:29]
            print(f"  {gpu_id:<11} | {machine:<19} | {priority_projects}")
    else:
        print("\nNo inactive GPUs found.")


# Removed chart generation function - not needed for simple HTML tables


# Removed number_format function - not needed for simple HTML tables


def send_email_report(
    html_content: str,
    to_email: str,
    from_email: str = "iaross@wisc.edu",
    smtp_server: str = "smtp.wiscmail.wisc.edu",
    smtp_port: int = 25,
    subject_prefix: str = "CHTC GPU Allocation",
    usage_percentages: dict[str, float] | None = None,
    lookback_hours: int | None = None,
    use_auth: bool = False,
    timeout: int = 30,
    debug: bool = False,
    device_stats: dict | None = None,
    analysis_type=None,
    month=None,
) -> bool:
    """
    Send HTML report via email using SMTP, matching mailx behavior.

    Args:
        html_content: HTML content to send
        to_email: Recipient email address(es) - can be comma-separated
        from_email: Sender email address
        smtp_server: SMTP server hostname
        smtp_port: SMTP server port
        subject_prefix: Subject line prefix
        usage_percentages: Dict of class usage percentages (e.g., {"Shared": 65.2, "Priority": 85.5})
        lookback_hours: Number of hours covered by the report (e.g., 24, 168)
        use_auth: Whether to use SMTP authentication
        timeout: Connection timeout in seconds
        debug: Enable debug output

    Returns:
        True if email sent successfully, False otherwise
    """
    try:
        # Parse comma-separated email addresses
        recipients = [email.strip() for email in to_email.split(",") if email.strip()]

        if not recipients:
            print("Error: No valid email addresses provided")
            return False

        # Create message
        msg = MIMEMultipart("alternative")
        today = datetime.datetime.now().strftime("%Y-%m-%d")

        # Build subject with lookback period and usage percentages
        subject = f"{subject_prefix} {today}"

        # Add lookback period
        if lookback_hours:
            if lookback_hours % (24 * 7) == 0 and lookback_hours >= (24 * 7):  # Exact weeks
                weeks = lookback_hours // (24 * 7)
                period_str = f"{weeks}w" if weeks > 1 else "1w"
            elif lookback_hours > 24 and lookback_hours % 24 == 0:  # Days for > 24h
                days = lookback_hours // 24
                period_str = f"{days}d"
            else:  # Hours for <= 24h or non-exact days
                period_str = f"{lookback_hours}h"
            if analysis_type == "monthly":
                # Use month if available, otherwise fall back to indicating it's monthly
                month_str = month if month else "Monthly"
                subject += f" {month_str}"
            else:
                subject += f" {period_str}"

        # Add usage percentages in order: Prioritized (Researcher), Prioritized (CHTC), Open Capacity, Backfill
        if usage_percentages:
            class_order = [
                "Priority-ResearcherOwned",  # Prioritized (Researcher Owned)
                "Priority-CHTCOwned",  # Prioritized (CHTC Owned)
                "Shared",  # Open Capacity
                "Backfill",  # Backfill (all types combined)
            ]
            usage_parts = []
            for class_name in class_order:
                if class_name in usage_percentages:
                    percentage = usage_percentages[class_name]
                    usage_parts.append(f"{percentage:.1f}%")
                elif class_name == "Backfill":
                    # For Backfill, combine all backfill types
                    backfill_types = ["Backfill-ResearcherOwned", "Backfill-CHTCOwned", "Backfill-OpenCapacity"]
                    total_claimed = 0
                    total_available = 0

                    if device_stats:
                        for backfill_type in backfill_types:
                            if backfill_type in device_stats:
                                device_data = device_stats[backfill_type]
                                if device_data:
                                    total_claimed += sum(stats["avg_claimed"] for stats in device_data.values())
                                    total_available += sum(
                                        stats["avg_total_available"] for stats in device_data.values()
                                    )

                        if total_available > 0:
                            combined_percentage = (total_claimed / total_available) * 100
                            usage_parts.append(f"{combined_percentage:.1f}%")

            if usage_parts:
                subject += f" ({' | '.join(usage_parts)})"

        msg["Subject"] = subject
        if debug:
            print(f"DEBUG: Email subject would be: {subject}")
        msg["From"] = from_email
        msg["To"] = ", ".join(recipients)

        # Attach HTML content
        html_part = MIMEText(html_content, "html")
        msg.attach(html_part)

        # Try multiple ports if connection fails (common university SMTP setup)
        ports_to_try = [smtp_port]
        if smtp_port != 25:
            ports_to_try.append(25)
        if smtp_port != 587:
            ports_to_try.append(587)

        last_error = None

        for port in ports_to_try:
            try:
                print(f"Connecting to SMTP server {smtp_server}:{port}...")

                # Send email - match mailx behavior more closely
                with smtplib.SMTP(smtp_server, port, timeout=timeout) as server:
                    # Enable debug output for troubleshooting if requested
                    if debug:
                        server.set_debuglevel(1)

                    # Try STARTTLS, but don't fail if not available (like mailx)
                    try:
                        server.starttls()
                        print("STARTTLS enabled")
                    except smtplib.SMTPNotSupportedError:
                        print("STARTTLS not supported, proceeding without encryption")
                    except Exception as e:
                        print(f"STARTTLS failed: {e}, proceeding without encryption")

                    # University SMTP servers often don't require auth from internal networks
                    # Only use auth if explicitly requested
                    if use_auth:
                        print("Note: Authentication not attempted (matching mailx behavior)")

                    server.send_message(msg, to_addrs=recipients)
                    print(f"Email sent successfully to {len(recipients)} recipient(s): {', '.join(recipients)}")
                    return True

            except (smtplib.SMTPException, OSError) as e:
                last_error = e
                print(f"Failed to connect on port {port}: {e}")
                continue

        # If we get here, all ports failed
        raise last_error or Exception("All SMTP ports failed")

    except smtplib.SMTPException as e:
        print(f"SMTP error sending email: {e}")
        return False
    except Exception as e:
        print(f"Error sending email: {e}")
        return False


def simple_markdown_to_html(markdown_text: str) -> str:
    """Convert simple markdown to HTML. Supports headers, bold, lists, and paragraphs."""
    lines = markdown_text.split("\n")
    html_lines = []
    in_list = False

    for line in lines:
        line = line.strip()
        if not line:
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append("")
            continue

        # Headers
        if line.startswith("# "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(f"<h2>{line[2:]}</h2>")
        # Bold text and convert **text:** to <strong>text:</strong>
        elif "**" in line:
            # Handle list items
            if line.startswith("- "):
                if not in_list:
                    html_lines.append("<ul>")
                    in_list = True
                line = line[2:]  # Remove "- "
                line = line.replace("**", "<strong>", 1).replace("**", "</strong>", 1)
                html_lines.append(f"<li>{line}</li>")
            else:
                # Regular paragraph with bold
                if in_list:
                    html_lines.append("</ul>")
                    in_list = False
                line = line.replace("**", "<strong>", 1).replace("**", "</strong>", 1)
                html_lines.append(f"<p>{line}</p>")
        # Regular list items
        elif line.startswith("- "):
            if not in_list:
                html_lines.append("<ul>")
                in_list = True
            html_lines.append(f"<li>{line[2:]}</li>")
        # Regular paragraphs
        else:
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(f"<p>{line}</p>")

    if in_list:
        html_lines.append("</ul>")

    return "\n".join(html_lines)


def load_methodology() -> str:
    """Load methodology from external markdown file and convert to HTML."""
    methodology_path = Path(__file__).parent / "methodology.md"
    try:
        with open(methodology_path, encoding="utf-8") as f:
            markdown_content = f.read()
        return simple_markdown_to_html(markdown_content)
    except FileNotFoundError:
        return "<p><em>Methodology file not found.</em></p>"
    except Exception as e:
        return f"<p><em>Error loading methodology: {e}</em></p>"


def generate_html_report(results: dict, output_file: str | None = None) -> str:
    """
    Generate a simple HTML report with tables from analysis results.

    Args:
        results: Analysis results dictionary
        output_file: Optional path to save HTML file

    Returns:
        HTML content as string
    """
    if "error" in results:
        return f"<html><body><h1>Error</h1><p>{results['error']}</p></body></html>"

    # Handle monthly summary - convert to regular results format and use existing HTML generation
    if "monthly_stats" in results:
        monthly_stats = results["monthly_stats"]
        if "error" in monthly_stats:
            return f"<html><body><h1>Monthly Summary Error</h1><p>{monthly_stats['error']}</p></body></html>"

        # Convert monthly stats to regular results format so we can reuse existing HTML generation
        regular_results = {
            "metadata": {
                "hours_back": monthly_stats["total_hours"],
                "start_time": monthly_stats.get("start_date"),
                "end_time": monthly_stats.get("end_date"),
                "num_intervals": monthly_stats["data_coverage"].get("unique_intervals", 0),
                "total_records": monthly_stats["data_coverage"].get("total_records", 0),
                "excluded_hosts": {},
                "filtered_hosts_info": {},
            }
        }

        # Copy the stats from monthly to regular format
        if "device_stats" in monthly_stats:
            regular_results["device_stats"] = monthly_stats["device_stats"]
        if "memory_stats" in monthly_stats:
            regular_results["memory_stats"] = monthly_stats["memory_stats"]
        if "h200_user_stats" in monthly_stats:
            regular_results["h200_user_stats"] = monthly_stats["h200_user_stats"]
        if "raw_data" in monthly_stats:
            regular_results["raw_data"] = monthly_stats["raw_data"]
        if "host_filter" in monthly_stats:
            regular_results["host_filter"] = monthly_stats["host_filter"]

        # Use existing HTML generation but with monthly title
        html_content = generate_html_report(regular_results, output_file)

        # Update the title to indicate it's a monthly report
        start_date_str = (
            monthly_stats["start_date"].strftime("%B %Y")
            if hasattr(monthly_stats["start_date"], "strftime")
            else str(monthly_stats["month"])
        )
        html_content = html_content.replace(
            "<title>CHTC GPU Allocation Report</title>", f"<title>CHTC Monthly GPU Report - {start_date_str}</title>"
        ).replace("<h1>CHTC GPU ALLOCATION REPORT</h1>", f"<h1>CHTC MONTHLY GPU REPORT - {start_date_str.upper()}</h1>")

        return html_content

    metadata = results["metadata"]

    # Start building HTML
    html_parts = []
    html_parts.append("<!DOCTYPE html>")
    html_parts.append("<html>")
    html_parts.append("<head>")
    html_parts.append("<title>CHTC GPU Allocation Report</title>")
    html_parts.append("</head>")
    html_parts.append("<body>")

    # Header
    html_parts.append("<h1>CHTC GPU ALLOCATION REPORT</h1>")
    # Simplified period format: just the lookback hours
    hours_back = metadata.get("hours_back", 24)
    hours_str = str(int(hours_back)) if hours_back == int(hours_back) else str(hours_back)
    hour_word = "hour" if hours_back == 1 else "hours"
    period_str = f"{hours_str} {hour_word}"
    html_parts.append(f"<p><strong>Period:</strong> {period_str}</p>")

    # Check if we have device stats for cluster summary
    device_stats = results.get("device_stats", {})
    class_totals = {}

    # Pre-calculate class totals if we have device stats
    if device_stats:
        for class_name in CLASS_ORDER:
            device_data = device_stats.get(class_name, {})
            if device_data:
                total_claimed = 0
                total_available = 0
                for _device_type, stats in device_data.items():
                    total_claimed += stats["avg_claimed"]
                    total_available += stats["avg_total_available"]

                if total_available > 0:
                    class_totals[class_name] = {
                        "claimed": total_claimed,
                        "total": total_available,
                        "percent": (total_claimed / total_available) * 100,
                    }

    # Cluster summary at the top with real slots and backfill slots separated
    if class_totals:
        # Separate real slots from backfill slots
        real_slot_classes = ["Priority-ResearcherOwned", "Priority-CHTCOwned", "Shared"]
        backfill_slot_classes = ["Backfill-ResearcherOwned", "Backfill-CHTCOwned", "Backfill-OpenCapacity"]

        # Calculate totals for real slots
        real_claimed = sum(class_totals[c]["claimed"] for c in real_slot_classes if c in class_totals)
        real_total = sum(class_totals[c]["total"] for c in real_slot_classes if c in class_totals)
        real_percent = (real_claimed / real_total * 100) if real_total > 0 else 0

        # Calculate totals for backfill slots
        backfill_claimed = sum(class_totals[c]["claimed"] for c in backfill_slot_classes if c in class_totals)
        backfill_total = sum(class_totals[c]["total"] for c in backfill_slot_classes if c in class_totals)
        backfill_percent = (backfill_claimed / backfill_total * 100) if backfill_total > 0 else 0

        # Calculate Open Capacity breakdown by performance tier (Flagship vs Standard)
        open_capacity_tiers = {"Flagship": {"claimed": 0, "total": 0}, "Standard": {"claimed": 0, "total": 0}}
        if "Shared" in device_stats:
            shared_device_data = device_stats["Shared"]
            for device_type, stats in shared_device_data.items():
                tier = get_gpu_performance_tier(device_type)
                open_capacity_tiers[tier]["claimed"] += stats["avg_claimed"]
                open_capacity_tiers[tier]["total"] += stats["avg_total_available"]

        # Calculate percentages for each tier
        for tier in open_capacity_tiers:
            if open_capacity_tiers[tier]["total"] > 0:
                open_capacity_tiers[tier]["percent"] = (
                    open_capacity_tiers[tier]["claimed"] / open_capacity_tiers[tier]["total"]
                ) * 100
            else:
                open_capacity_tiers[tier]["percent"] = 0

        # Real Slots Table
        html_parts.append("<h2>Real Slots</h2>")
        html_parts.append("<table border='1' style='margin-top: 20px;'>")
        html_parts.append(
            "<tr style='background-color: #e0e0e0;'><th>Class</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>"
        )

        # Total row for real slots
        html_parts.append("<tr style='background-color: #d0d0d0; font-weight: bold;'>")
        html_parts.append("<td style='font-weight: bold;'>TOTAL</td>")
        html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{real_percent:.1f}%</td>")
        html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{real_claimed:.1f}</td>")
        html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{real_total:.1f}</td>")
        html_parts.append("</tr>")

        # Individual real slot classes (with Open Capacity broken into tiers)
        for class_name in real_slot_classes:
            if class_name in class_totals:
                if class_name == "Shared":
                    # Show Open Capacity broken down by tier
                    for tier in ["Flagship", "Standard"]:
                        tier_data = open_capacity_tiers[tier]
                        if tier_data["total"] > 0:
                            html_parts.append("<tr>")
                            html_parts.append(f"<td style='font-weight: bold;'>Open Capacity ({tier})</td>")
                            html_parts.append(
                                f"<td style='text-align: right; font-weight: bold;'>{tier_data['percent']:.1f}%</td>"
                            )
                            html_parts.append(
                                f"<td style='text-align: right; font-weight: bold;'>{tier_data['claimed']:.1f}</td>"
                            )
                            html_parts.append(
                                f"<td style='text-align: right; font-weight: bold;'>{tier_data['total']:.1f}</td>"
                            )
                            html_parts.append("</tr>")
                else:
                    totals = class_totals[class_name]
                    html_parts.append("<tr>")
                    html_parts.append(f"<td style='font-weight: bold;'>{get_display_name(class_name)}</td>")
                    html_parts.append(
                        f"<td style='text-align: right; font-weight: bold;'>{totals['percent']:.1f}%</td>"
                    )
                    html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{totals['claimed']:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{totals['total']:.1f}</td>")
                    html_parts.append("</tr>")

        # Add separator row and backfill slots to the same table
        html_parts.append("<tr style='background-color: #f0f0f0;'><td colspan='4' style='height: 10px;'></td></tr>")

        # Backfill section header
        html_parts.append("<tr style='background-color: #d0d0d0; font-weight: bold;'>")
        html_parts.append("<td style='font-weight: bold;'>BACKFILL TOTAL</td>")
        html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{backfill_percent:.1f}%</td>")
        html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{backfill_claimed:.1f}</td>")
        html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{backfill_total:.1f}</td>")
        html_parts.append("</tr>")

        # Individual backfill slot classes
        for class_name in backfill_slot_classes:
            if class_name in class_totals:
                totals = class_totals[class_name]
                html_parts.append("<tr>")
                html_parts.append(f"<td style='font-weight: bold;'>{get_display_name(class_name)}</td>")
                html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{totals['percent']:.1f}%</td>")
                html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{totals['claimed']:.1f}</td>")
                html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{totals['total']:.1f}</td>")
                html_parts.append("</tr>")

        html_parts.append("</table>")

        # Real Slots by Memory Category Table
        if "memory_stats" in results:
            memory_stats = results["memory_stats"]
            if memory_stats:
                html_parts.append("<h2>Real Slots by Memory Category (filtered)</h2>")
                html_parts.append("<table border='1' style='margin-top: 20px;'>")
                html_parts.append(
                    "<tr style='background-color: #e0e0e0;'><th>Memory Category</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>"
                )

                # Calculate totals for memory categories - include drained
                memory_total_allocated = sum(
                    stats["avg_claimed"] + stats.get("avg_drained", 0.0) for stats in memory_stats.values()
                )
                memory_total_available = sum(stats["avg_total_available"] for stats in memory_stats.values())
                memory_total_percent = (
                    (memory_total_allocated / memory_total_available * 100) if memory_total_available > 0 else 0
                )

                # Total row for memory categories
                html_parts.append("<tr style='background-color: #d0d0d0; font-weight: bold;'>")
                html_parts.append("<td style='font-weight: bold;'>TOTAL</td>")
                html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{memory_total_percent:.1f}%</td>")
                html_parts.append(
                    f"<td style='text-align: right; font-weight: bold;'>{memory_total_allocated:.1f}</td>"
                )
                html_parts.append(
                    f"<td style='text-align: right; font-weight: bold;'>{memory_total_available:.1f}</td>"
                )
                html_parts.append("</tr>")

                # Sort memory categories by numerical value
                def sort_memory_categories(categories):
                    def get_sort_key(cat):
                        if cat == "Unknown":
                            return 999  # Put Unknown at the end
                        elif cat.startswith("<"):
                            # Handle <48GB - extract the number after <
                            return float(cat[1:-2])  # Remove "<" and "GB"
                        elif cat.startswith(">"):
                            # Handle >80GB - extract the number after >
                            return float(cat[1:-2]) + 0.1  # Add 0.1 to sort after exact values
                        elif cat.endswith("GB+"):
                            return float(cat[:-3])  # Remove "GB+" suffix
                        elif cat.endswith("GB"):
                            if "-" in cat:
                                # Handle ranges like "10-12GB"
                                return float(cat.split("-")[0])
                            else:
                                return float(cat[:-2])  # Remove "GB" suffix
                        else:
                            return 0  # Fallback

                    return sorted(categories, key=get_sort_key)

                # Individual memory categories (sorted by memory size)
                sorted_memory_cats = sort_memory_categories(memory_stats.keys())
                for memory_cat in sorted_memory_cats:
                    stats = memory_stats[memory_cat]
                    # Allocated = claimed + drained
                    allocated = stats["avg_claimed"] + stats.get("avg_drained", 0.0)
                    allocated_pct = (
                        (allocated / stats["avg_total_available"] * 100) if stats["avg_total_available"] > 0 else 0
                    )
                    html_parts.append("<tr>")
                    html_parts.append(f"<td style='font-weight: bold;'>{memory_cat}</td>")
                    html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{allocated_pct:.1f}%</td>")
                    html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{allocated:.1f}</td>")
                    html_parts.append(
                        f"<td style='text-align: right; font-weight: bold;'>{stats['avg_total_available']:.1f}</td>"
                    )
                    html_parts.append("</tr>")

                html_parts.append("</table>")

    if "allocation_stats" in results:
        html_parts.append("<h2>Allocation Summary</h2>")
        html_parts.append("<table border='1'>")
        html_parts.append(
            "<tr><th>Class</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>"
        )

        allocation_stats = results["allocation_stats"]

        for class_name in CLASS_ORDER:
            if class_name in allocation_stats:
                stats = allocation_stats[class_name]
                html_parts.append("<tr>")
                html_parts.append(f"<td>{get_display_name(class_name)}</td>")
                html_parts.append(f"<td style='text-align: right'>{stats['allocation_usage_percent']:.1f}%</td>")
                html_parts.append(f"<td style='text-align: right'>{stats['avg_claimed']:.1f}</td>")
                html_parts.append(f"<td style='text-align: right'>{stats['avg_total_available']:.1f}</td>")
                html_parts.append("</tr>")

        html_parts.append("</table>")
    # Device stats tables
    elif "device_stats" in results:
        # H200 Usage by Slot Type (positioned after backfill slots, before device type details)
        if "h200_user_stats" in results:
            h200_stats = results["h200_user_stats"]
            if h200_stats:
                html_parts.append("<h2>H200 Usage by Slot Type and User</h2>")

                # First, aggregate data by slot type
                slot_type_totals = {}
                slot_type_users = {}

                for user, user_data in h200_stats.items():
                    for slot_type, slot_data in user_data["slot_breakdown"].items():
                        if slot_type not in slot_type_totals:
                            slot_type_totals[slot_type] = 0
                            slot_type_users[slot_type] = []

                        slot_type_totals[slot_type] += slot_data["gpu_hours"]
                        slot_type_users[slot_type].append(
                            {"user": user, "gpu_hours": slot_data["gpu_hours"], "percentage": slot_data["percentage"]}
                        )

                # Sort slot types by total GPU hours (descending)
                sorted_slot_types = sorted(slot_type_totals.items(), key=lambda x: x[1], reverse=True)

                # Single table with slot type totals and user breakdowns
                html_parts.append("<table border='1' style='margin-top: 20px;'>")
                html_parts.append(
                    "<tr style='background-color: #e0e0e0;'><th>Slot Type / User</th><th>GPU-Hours</th><th>% of Total</th></tr>"
                )

                total_gpu_hours = sum(slot_type_totals.values())

                for slot_type, total_hours in sorted_slot_types:
                    display_name = get_display_name(slot_type)
                    user_count = len(slot_type_users[slot_type])
                    percentage = (total_hours / total_gpu_hours * 100) if total_gpu_hours > 0 else 0

                    # Slot type total row
                    html_parts.append("<tr style='background-color: #f0f0f0;'>")
                    html_parts.append(f"<td style='font-weight: bold;'>{display_name} ({user_count} users)</td>")
                    html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{total_hours:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{percentage:.1f}%</td>")
                    html_parts.append("</tr>")

                    # User breakdown rows beneath the slot type total
                    users = slot_type_users[slot_type]
                    users.sort(key=lambda x: x["gpu_hours"], reverse=True)

                    for user_info in users:
                        user = user_info["user"]
                        gpu_hours = user_info["gpu_hours"]
                        user_total_percentage = (gpu_hours / total_gpu_hours * 100) if total_gpu_hours > 0 else 0

                        html_parts.append("<tr>")
                        html_parts.append(f"<td style='padding-left: 30px;'>{user}</td>")
                        html_parts.append(f"<td style='text-align: right;'>{gpu_hours:.1f}</td>")
                        html_parts.append(f"<td style='text-align: right;'>{user_total_percentage:.1f}%</td>")
                        html_parts.append("</tr>")

                html_parts.append("</table>")

        # Backfill Usage by Slot Type and User
        if "backfill_user_stats" in results:
            backfill_stats = results["backfill_user_stats"]
            if backfill_stats:
                html_parts.append("<h2>Backfill Usage by Slot Type and User (filtered)</h2>")

                # First, aggregate data by slot type
                slot_type_totals = {}
                slot_type_users = {}

                for user, user_data in backfill_stats.items():
                    for slot_type, slot_data in user_data["slot_breakdown"].items():
                        if slot_type not in slot_type_totals:
                            slot_type_totals[slot_type] = 0
                            slot_type_users[slot_type] = []
                        slot_type_totals[slot_type] += slot_data["gpu_hours"]
                        slot_type_users[slot_type].append(
                            {"user": user, "gpu_hours": slot_data["gpu_hours"], "percentage": slot_data["percentage"]}
                        )

                # Sort slot types by total GPU hours (descending)
                sorted_slot_types = sorted(slot_type_totals.items(), key=lambda x: x[1], reverse=True)

                # Single table with slot type totals and user breakdowns
                html_parts.append("<table border='1' style='margin-top: 20px;'>")
                html_parts.append(
                    "<tr style='background-color: #e0e0e0;'><th>Slot Type / User</th><th>GPU-Hours</th><th>% of Total</th></tr>"
                )

                total_gpu_hours = sum(slot_type_totals.values())
                for slot_type, total_hours in sorted_slot_types:
                    display_name = get_display_name(slot_type)
                    user_count = len(slot_type_users[slot_type])
                    percentage = (total_hours / total_gpu_hours * 100) if total_gpu_hours > 0 else 0

                    # Slot type total row
                    html_parts.append("<tr style='background-color: #f0f0f0;'>")
                    html_parts.append(f"<td style='font-weight: bold;'>{display_name} ({user_count} users)</td>")
                    html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{total_hours:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{percentage:.1f}%</td>")
                    html_parts.append("</tr>")

                    # User breakdown rows beneath the slot type total
                    users = slot_type_users[slot_type]
                    users.sort(key=lambda x: x["gpu_hours"], reverse=True)
                    for user_info in users:
                        user = user_info["user"]
                        gpu_hours = user_info["gpu_hours"]
                        user_total_percentage = (gpu_hours / total_gpu_hours * 100) if total_gpu_hours > 0 else 0
                        html_parts.append("<tr>")
                        html_parts.append(f"<td style='padding-left: 30px;'>{user}</td>")
                        html_parts.append(f"<td style='text-align: right;'>{gpu_hours:.1f}</td>")
                        html_parts.append(f"<td style='text-align: right;'>{user_total_percentage:.1f}%</td>")
                        html_parts.append("</tr>")

                html_parts.append("</table>")

        # Machines with Zero Active GPUs
        if "zero_active_machines" in results:
            zero_active_data = results["zero_active_machines"]
            if zero_active_data and zero_active_data["machines"]:
                html_parts.append("<h2>Machines with Zero Active Primary Slot GPUs</h2>")
                html_parts.append(
                    f"<p><strong>Total machines:</strong> {zero_active_data['summary']['total_machines']} | <strong>Total idle GPUs:</strong> {zero_active_data['summary']['total_gpus_idle']}</p>"
                )

                # Single table with all machines
                html_parts.append("<table border='1'>")
                html_parts.append(
                    "<tr><th>Machine</th><th>GPU Type</th><th>GPUs</th><th>Avg Backfill</th><th>Prioritized Projects</th></tr>"
                )

                # Print all machines in a single table, sorted by machine name
                for machine_info in sorted(zero_active_data["machines"], key=lambda x: x["machine"]):
                    machine = machine_info["machine"]
                    gpu_model = machine_info["gpu_model"]
                    total_gpus = machine_info["total_gpus"]
                    avg_backfill = machine_info["avg_backfill_claimed"]
                    prioritized = machine_info["prioritized_projects"]

                    # Shorten GPU model name for better display
                    short_gpu_name = get_human_readable_device_name(gpu_model)

                    if prioritized:
                        projects_str = ", ".join(sorted(prioritized))
                    else:
                        projects_str = "Open Capacity"

                    html_parts.append("<tr>")
                    html_parts.append(f"<td>{machine}</td>")
                    html_parts.append(f"<td>{short_gpu_name}</td>")
                    html_parts.append(f"<td style='text-align: right'>{total_gpus}</td>")
                    html_parts.append(f"<td style='text-align: right'>{avg_backfill:.1f}</td>")
                    html_parts.append(f"<td>{projects_str}</td>")
                    html_parts.append("</tr>")

                html_parts.append("</table>")

        # MIG Hosts Summary
        if "mig_hosts" in results and results["mig_hosts"]:
            mig_hosts = results["mig_hosts"]
            html_parts.append("<h2>Hosts with MIG Devices</h2>")
            html_parts.append("<table border='1' style='margin-top: 20px;'>")
            html_parts.append(
                "<tr style='background-color: #e0e0e0;'>"
                "<th>Machine</th><th>MIG Device</th>"
                "<th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th>"
                "</tr>"
            )
            for entry in mig_hosts:
                short_name = get_human_readable_device_name(entry["mig_device"])
                html_parts.append("<tr>")
                html_parts.append(f"<td>{entry['machine']}</td>")
                html_parts.append(f"<td>{short_name}</td>")
                html_parts.append(f"<td style='text-align: right'>{entry['utilization_pct']:.1f}%</td>")
                html_parts.append(f"<td style='text-align: right'>{entry['avg_claimed']:.1f}</td>")
                html_parts.append(f"<td style='text-align: right'>{entry['avg_total']:.1f}</td>")
                html_parts.append("</tr>")
            html_parts.append("</table>")

        html_parts.append("<h2>Usage by Device Type (filtered)</h2>")

        for class_name in CLASS_ORDER:
            device_data = device_stats.get(class_name, {})
            if device_data:
                html_parts.append(f"<h3>{get_display_name(class_name)}</h3>")
                html_parts.append("<table border='1'>")
                html_parts.append(
                    "<tr><th>Device Type</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>"
                )

                # Calculate totals first - include drained in allocated
                total_allocated = 0
                total_available = 0
                for _device_type, stats in sorted(device_data.items()):
                    # Allocated = claimed + drained
                    allocated = stats["avg_claimed"] + stats.get("avg_drained", 0.0)
                    total_allocated += allocated
                    total_available += stats["avg_total_available"]

                # Add total row first
                if total_available > 0:
                    total_percent = (total_allocated / total_available) * 100
                    html_parts.append("<tr style='font-weight: bold; background-color: #f0f0f0;'>")
                    html_parts.append("<td>TOTAL</td>")

                    html_parts.append(f"<td style='text-align: right'>{total_percent:.1f}%</td>")
                    html_parts.append(f"<td style='text-align: right'>{total_allocated:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right'>{total_available:.1f}</td>")
                    html_parts.append("</tr>")

                    class_totals[class_name] = {
                        "claimed": total_allocated,
                        "total": total_available,
                        "percent": total_percent,
                    }

                # Add individual device rows (sorted alphabetically)
                for device_type, stats in sorted(device_data.items()):
                    short_name = get_human_readable_device_name(device_type)
                    # Allocated = claimed + drained
                    allocated = stats["avg_claimed"] + stats.get("avg_drained", 0.0)
                    allocated_pct = (
                        (allocated / stats["avg_total_available"] * 100) if stats["avg_total_available"] > 0 else 0
                    )
                    html_parts.append("<tr>")
                    html_parts.append(f"<td>{short_name}</td>")

                    html_parts.append(f"<td style='text-align: right'>{allocated_pct:.1f}%</td>")
                    html_parts.append(f"<td style='text-align: right'>{allocated:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['avg_total_available']:.1f}</td>")
                    html_parts.append("</tr>")

                html_parts.append("</table>")

        # Machine categories table for enhanced view
        if "machine_categories" in results:
            html_parts.append("<h2>Machine Categories</h2>")
            machine_categories = results["machine_categories"]

            for category, machines in machine_categories.items():
                if machines:  # Only show categories that have machines
                    html_parts.append(f"<h3>{category} ({len(machines)} machines)</h3>")
                    html_parts.append("<table border='1'>")
                    html_parts.append("<tr><th>Machine</th></tr>")

                    for machine in machines:
                        html_parts.append("<tr>")
                        html_parts.append(f"<td>{machine}</td>")
                        html_parts.append("</tr>")

                    html_parts.append("</table>")

    # Device stats tables
    elif "device_stats" in results:
        html_parts.append("<h2>Usage by Device Type (filtered)</h2>")

        device_stats = results["device_stats"]
        class_totals = {}

        # Define the order: Open Capacity, Prioritized Service, Backfill
        class_order = ["Shared", "Priority", "Backfill"]  # Internal names

        for class_name in class_order:
            device_data = device_stats.get(class_name, {})
            if device_data:
                html_parts.append(f"<h3>{get_display_name(class_name)}</h3>")
                html_parts.append("<table border='1'>")
                html_parts.append(
                    "<tr><th>Device Type</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>"
                )

                # Calculate totals first - include drained in allocated
                total_allocated = 0
                total_available = 0
                for _device_type, stats in sorted(device_data.items()):
                    # Allocated = claimed + drained
                    allocated = stats["avg_claimed"] + stats.get("avg_drained", 0.0)
                    total_allocated += allocated
                    total_available += stats["avg_total_available"]

                # Add total row first
                if total_available > 0:
                    total_percent = (total_allocated / total_available) * 100
                    html_parts.append("<tr style='font-weight: bold; background-color: #f0f0f0;'>")
                    html_parts.append("<td>TOTAL</td>")

                    html_parts.append(f"<td style='text-align: right'>{total_percent:.1f}%</td>")
                    html_parts.append(f"<td style='text-align: right'>{total_allocated:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right'>{total_available:.1f}</td>")
                    html_parts.append("</tr>")

                    class_totals[class_name] = {
                        "claimed": total_allocated,
                        "total": total_available,
                        "percent": total_percent,
                    }

                # Add individual device rows (sorted alphabetically)
                for device_type, stats in sorted(device_data.items()):
                    short_name = get_human_readable_device_name(device_type)
                    # Allocated = claimed + drained
                    allocated = stats["avg_claimed"] + stats.get("avg_drained", 0.0)
                    allocated_pct = (
                        (allocated / stats["avg_total_available"] * 100) if stats["avg_total_available"] > 0 else 0
                    )
                    html_parts.append("<tr>")
                    html_parts.append(f"<td>{short_name}</td>")

                    html_parts.append(f"<td style='text-align: right'>{allocated_pct:.1f}%</td>")
                    html_parts.append(f"<td style='text-align: right'>{allocated:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['avg_total_available']:.1f}</td>")
                    html_parts.append("</tr>")

                html_parts.append("</table>")

        # Cluster summary with real slots and backfill slots separated
        if class_totals:
            # Separate real slots from backfill slots (using simplified class names)
            real_slot_classes = ["Shared", "Priority"]
            backfill_slot_classes = ["Backfill"]

            # Calculate totals for real slots
            real_claimed = sum(class_totals[c]["claimed"] for c in real_slot_classes if c in class_totals)
            real_total = sum(class_totals[c]["total"] for c in real_slot_classes if c in class_totals)
            real_percent = (real_claimed / real_total * 100) if real_total > 0 else 0

            # Calculate totals for backfill slots
            backfill_claimed = sum(class_totals[c]["claimed"] for c in backfill_slot_classes if c in class_totals)
            backfill_total = sum(class_totals[c]["total"] for c in backfill_slot_classes if c in class_totals)
            backfill_percent = (backfill_claimed / backfill_total * 100) if backfill_total > 0 else 0

            # Real Slots Table
            html_parts.append("<h2>Real Slots</h2>")
            html_parts.append("<table border='1'>")
            html_parts.append(
                "<tr><th>Class</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>"
            )

            # Total row for real slots
            html_parts.append("<tr style='font-weight: bold; background-color: #f0f0f0;'>")
            html_parts.append("<td>TOTAL</td>")
            html_parts.append(f"<td style='text-align: right'>{real_percent:.1f}%</td>")
            html_parts.append(f"<td style='text-align: right'>{real_claimed:.1f}</td>")
            html_parts.append(f"<td style='text-align: right'>{real_total:.1f}</td>")
            html_parts.append("</tr>")

            # Individual real slot classes
            for class_name in real_slot_classes:
                if class_name in class_totals:
                    stats = class_totals[class_name]
                    html_parts.append("<tr>")
                    html_parts.append(f"<td>{get_display_name(class_name)}</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['percent']:.1f}%</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['claimed']:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['total']:.1f}</td>")
                    html_parts.append("</tr>")
            html_parts.append("</table>")

            # Real Slots by Memory Category Table
            if "memory_stats" in results:
                memory_stats = results["memory_stats"]
                if memory_stats:
                    html_parts.append("<h2>Real Slots by Memory Category (filtered)</h2>")
                    html_parts.append("<table border='1'>")
                    html_parts.append(
                        "<tr><th>Memory Category</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>"
                    )

                    # Calculate totals for memory categories - include drained
                    memory_total_allocated = sum(
                        stats["avg_claimed"] + stats.get("avg_drained", 0.0) for stats in memory_stats.values()
                    )
                    memory_total_available = sum(stats["avg_total_available"] for stats in memory_stats.values())
                    memory_total_percent = (
                        (memory_total_allocated / memory_total_available * 100) if memory_total_available > 0 else 0
                    )

                    # Total row for memory categories
                    html_parts.append("<tr style='font-weight: bold; background-color: #f0f0f0;'>")
                    html_parts.append("<td>TOTAL</td>")
                    html_parts.append(f"<td style='text-align: right'>{memory_total_percent:.1f}%</td>")
                    html_parts.append(f"<td style='text-align: right'>{memory_total_allocated:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right'>{memory_total_available:.1f}</td>")
                    html_parts.append("</tr>")

                    # Sort memory categories by numerical value
                    def sort_memory_categories(categories):
                        def get_sort_key(cat):
                            if cat == "Unknown":
                                return 999  # Put Unknown at the end
                            elif cat.endswith("GB+"):
                                return float(cat[:-3])  # Remove "GB+" suffix
                            elif cat.endswith("GB"):
                                if "-" in cat:
                                    # Handle ranges like "10-12GB"
                                    return float(cat.split("-")[0])
                                else:
                                    return float(cat[:-2])  # Remove "GB" suffix
                            else:
                                return 0  # Fallback

                        return sorted(categories, key=get_sort_key)

                    # Individual memory categories (sorted by memory size)
                    sorted_memory_cats = sort_memory_categories(memory_stats.keys())
                    for memory_cat in sorted_memory_cats:
                        stats = memory_stats[memory_cat]
                        # Allocated = claimed + drained
                        allocated = stats["avg_claimed"] + stats.get("avg_drained", 0.0)
                        allocated_pct = (
                            (allocated / stats["avg_total_available"] * 100) if stats["avg_total_available"] > 0 else 0
                        )
                        html_parts.append("<tr>")
                        html_parts.append(f"<td>{memory_cat}</td>")
                        html_parts.append(f"<td style='text-align: right'>{allocated_pct:.1f}%</td>")
                        html_parts.append(f"<td style='text-align: right'>{allocated:.1f}</td>")
                        html_parts.append(f"<td style='text-align: right'>{stats['avg_total_available']:.1f}</td>")
                        html_parts.append("</tr>")

                    html_parts.append("</table>")

            # Backfill Slots Table
            html_parts.append("<h2>Backfill Slots</h2>")
            html_parts.append("<table border='1'>")
            html_parts.append(
                "<tr><th>Class</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>"
            )

            # Total row for backfill slots
            html_parts.append("<tr style='font-weight: bold; background-color: #f0f0f0;'>")
            html_parts.append("<td>TOTAL</td>")
            html_parts.append(f"<td style='text-align: right'>{backfill_percent:.1f}%</td>")
            html_parts.append(f"<td style='text-align: right'>{backfill_claimed:.1f}</td>")
            html_parts.append(f"<td style='text-align: right'>{backfill_total:.1f}</td>")
            html_parts.append("</tr>")

            # Individual backfill slot classes
            for class_name in backfill_slot_classes:
                if class_name in class_totals:
                    stats = class_totals[class_name]
                    html_parts.append("<tr>")
                    html_parts.append(f"<td>{get_display_name(class_name)}</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['percent']:.1f}%</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['claimed']:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['total']:.1f}</td>")
                    html_parts.append("</tr>")
            html_parts.append("</table>")

    # Excluded hosts
    excluded_hosts = metadata.get("excluded_hosts", {})
    if excluded_hosts:
        html_parts.append("<h2>Excluded Hosts</h2>")
        html_parts.append("<table border='1'>")
        html_parts.append("<tr><th>Host</th><th>Reason</th></tr>")
        for host, reason in excluded_hosts.items():
            html_parts.append(f"<tr><td>{host}</td><td>{reason}</td></tr>")
        html_parts.append("</table>")

    # Add methodology section from external file
    methodology_html = load_methodology()
    html_parts.append("<div style='background-color: #f9f9f9; padding: 15px; border-radius: 5px; margin-top: 20px;'>")
    html_parts.append(methodology_html)
    html_parts.append("</div>")

    # Add time range information at the end
    html_parts.append(
        "<div style='background-color: #f0f0f0; padding: 10px; margin-top: 20px; text-align: center; font-style: italic; color: #666;'>"
    )
    if metadata.get("is_monthly", False):
        # For monthly reports, show the month
        monthly_period = metadata.get("monthly_period", "Unknown Period")
        html_parts.append(f"<strong>Data Period:</strong> {monthly_period}")
    else:
        # For regular reports, show start and end times
        start_time = metadata.get("start_time")
        end_time = metadata.get("end_time")
        if start_time and end_time:
            # Format timestamps nicely
            if hasattr(start_time, "strftime"):
                start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
            else:
                start_str = str(start_time)
            if hasattr(end_time, "strftime"):
                end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
            else:
                end_str = str(end_time)
            html_parts.append(f"<strong>Data Period:</strong> {start_str} to {end_str}")
        else:
            # Fallback to hours_back if timestamps not available
            hours_back = metadata.get("hours_back", 24)
            html_parts.append(f"<strong>Data Period:</strong> Last {hours_back} hours")
    html_parts.append("</div>")

    # Add runtime footer
    html_parts.append("<hr>")
    html_parts.append("<div style='font-size: 12px; color: #666; text-align: center; margin-top: 20px;'>")

    # Add runtime information if available
    if "analysis_runtime_seconds" in metadata:
        runtime = metadata["analysis_runtime_seconds"]
        if runtime < 60:
            runtime_str = f"{runtime:.2f} seconds"
        else:
            minutes = int(runtime // 60)
            seconds = runtime % 60
            runtime_str = f"{minutes}m {seconds:.1f}s"
        html_parts.append(f"Report generated in {runtime_str}")

        # Add generation timestamp if available
        if "analysis_end_datetime" in metadata:
            end_time = metadata["analysis_end_datetime"]
            # Parse ISO format and format for display
            from datetime import datetime as dt_parser

            dt = dt_parser.fromisoformat(end_time.replace("Z", "+00:00"))
            time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            html_parts.append(f" | Generated: {time_str}")

    html_parts.append("</div>")

    html_parts.append("</body>")
    html_parts.append("</html>")

    html_content = "\n".join(html_parts)

    # Save to file if specified
    if output_file:
        try:
            with open(output_file, "w") as f:
                f.write(html_content)
            print(f"HTML report saved to: {output_file}")
        except Exception as e:
            import sys

            print(f"Error saving HTML report to {output_file}: {e}", file=sys.stderr)
            # Fall back to stdout
            print(html_content)
            return html_content

    return html_content


def print_analysis_results(results: dict, output_format: str = "text", output_file: str | None = None):
    """Print analysis results in a formatted way.

    Args:
        results: Analysis results dictionary
        output_format: Output format ('text' or 'html')
        output_file: Optional file path to save output
    """
    if output_format == "html":
        html_content = generate_html_report(results, output_file)
        if not output_file:
            print(html_content)
        return

    # Original text output
    if "error" in results:
        print(results["error"])
        return

    # Handle monthly summary - convert to regular results format and use existing text output
    if "monthly_stats" in results:
        monthly_stats = results["monthly_stats"]
        if "error" in monthly_stats:
            print(monthly_stats["error"])
            return

        # Convert monthly stats to regular results format so we can reuse existing text output
        regular_results = {
            "metadata": {
                "hours_back": monthly_stats["total_hours"],
                "start_time": monthly_stats.get("start_date"),
                "end_time": monthly_stats.get("end_date"),
                "num_intervals": monthly_stats["data_coverage"].get("unique_intervals", 0),
                "total_records": monthly_stats["data_coverage"].get("total_records", 0),
                "excluded_hosts": {},
                "filtered_hosts_info": {},
            }
        }

        # Copy the stats from monthly to regular format
        if "device_stats" in monthly_stats:
            regular_results["device_stats"] = monthly_stats["device_stats"]
        if "memory_stats" in monthly_stats:
            regular_results["memory_stats"] = monthly_stats["memory_stats"]
        if "h200_user_stats" in monthly_stats:
            regular_results["h200_user_stats"] = monthly_stats["h200_user_stats"]
        if "raw_data" in monthly_stats:
            regular_results["raw_data"] = monthly_stats["raw_data"]
        if "host_filter" in monthly_stats:
            regular_results["host_filter"] = monthly_stats["host_filter"]

        # Override results with converted monthly data and continue with normal text processing
        results = regular_results

        # Mark as monthly for different header formatting
        results["metadata"]["is_monthly"] = True
        results["metadata"]["monthly_period"] = (
            monthly_stats["start_date"].strftime("%B %Y")
            if hasattr(monthly_stats["start_date"], "strftime")
            else str(monthly_stats["month"])
        )

    metadata = results["metadata"]

    # Print appropriate header based on type
    if metadata.get("is_monthly", False):
        print(f"\n{'=' * 70}")
        print(f"{'CHTC MONTHLY GPU REPORT - ' + metadata['monthly_period'].upper():^70}")
        print(f"{'=' * 70}")
        print(f"Period: {metadata['monthly_period']}")
        print(f"{'=' * 70}")
    else:
        print(f"\n{'=' * 70}")
        print(f"{'CHTC GPU UTILIZATION REPORT':^70}")
        print(f"{'=' * 70}")
        # Simplified period format for console: just the lookback hours
        hours_back = metadata.get("hours_back", 24)
        hours_str = str(int(hours_back)) if hours_back == int(hours_back) else str(hours_back)
        hour_word = "hour" if hours_back == 1 else "hours"
        period_str = f"{hours_str} {hour_word}"
        print(f"Period: {period_str}")
        print(f"{'=' * 70}")

    # Calculate cluster summary first if we have device stats
    grand_totals = {}
    if "device_stats" in results:
        device_stats = results["device_stats"]
        class_order = CLASS_ORDER

        for class_name in class_order:
            device_data = device_stats.get(class_name, {})
            if device_data:
                total_claimed = 0
                total_available = 0
                for _device_type, stats in device_data.items():
                    total_claimed += stats["avg_claimed"]
                    total_available += stats["avg_total_available"]

                if total_available > 0:
                    grand_totals[class_name] = {
                        "claimed": total_claimed,
                        "total": total_available,
                        "percent": (total_claimed / total_available) * 100,
                    }

    # Show cluster summary at the top with real slots and backfill slots separated
    if grand_totals:
        # Separate real slots from backfill slots
        real_slot_classes = ["Priority-ResearcherOwned", "Priority-CHTCOwned", "Shared"]
        backfill_slot_classes = ["Backfill-ResearcherOwned", "Backfill-CHTCOwned", "Backfill-OpenCapacity"]

        # Calculate totals for real slots
        real_claimed = sum(grand_totals[c]["claimed"] for c in real_slot_classes if c in grand_totals)
        real_total = sum(grand_totals[c]["total"] for c in real_slot_classes if c in grand_totals)
        real_percent = (real_claimed / real_total * 100) if real_total > 0 else 0

        # Calculate totals for backfill slots
        backfill_claimed = sum(grand_totals[c]["claimed"] for c in backfill_slot_classes if c in grand_totals)
        backfill_total = sum(grand_totals[c]["total"] for c in backfill_slot_classes if c in grand_totals)
        backfill_percent = (backfill_claimed / backfill_total * 100) if backfill_total > 0 else 0

        print("\nREAL SLOTS:")
        print(f"{'-' * 70}")
        print(f"  TOTAL: {real_percent:.1f}% ({real_claimed:.1f}/{real_total:.1f} GPUs)")
        print(f"{'-' * 70}")
        for class_name in real_slot_classes:
            if class_name in grand_totals:
                totals = grand_totals[class_name]
                print(
                    f"  {get_display_name(class_name)}: {totals['percent']:.1f}% "
                    f"({totals['claimed']:.1f}/{totals['total']:.1f} GPUs)"
                )

        # Real Slots by Memory Category
        if "memory_stats" in results:
            memory_stats = results["memory_stats"]
            if memory_stats:
                # Include drained in allocated
                memory_total_allocated = sum(
                    stats["avg_claimed"] + stats.get("avg_drained", 0.0) for stats in memory_stats.values()
                )
                memory_total_available = sum(stats["avg_total_available"] for stats in memory_stats.values())
                memory_total_percent = (
                    (memory_total_allocated / memory_total_available * 100) if memory_total_available > 0 else 0
                )

                print("\nREAL SLOTS BY MEMORY CATEGORY (filtered):")
                print(f"{'-' * 80}")
                print(
                    f"  TOTAL: {memory_total_percent:.1f}% ({memory_total_allocated:.1f}/{memory_total_available:.1f} GPUs)"
                )
                print(f"{'-' * 80}")

                # Sort memory categories by numerical value
                def sort_memory_categories(categories):
                    def get_sort_key(cat):
                        if cat == "Unknown":
                            return 999  # Put Unknown at the end
                        elif cat.startswith("<"):
                            # Handle <48GB - extract the number after <
                            return float(cat[1:-2])  # Remove "<" and "GB"
                        elif cat.startswith(">"):
                            # Handle >80GB - extract the number after >
                            return float(cat[1:-2]) + 0.1  # Add 0.1 to sort after exact values
                        elif cat.endswith("GB+"):
                            return float(cat[:-3])  # Remove "GB+" suffix
                        elif cat.endswith("GB"):
                            if "-" in cat:
                                # Handle ranges like "10-12GB"
                                return float(cat.split("-")[0])
                            else:
                                return float(cat[:-2])  # Remove "GB" suffix
                        else:
                            return 0  # Fallback

                    return sorted(categories, key=get_sort_key)

                # Individual memory categories (sorted by memory size)
                sorted_memory_cats = sort_memory_categories(memory_stats.keys())
                for memory_cat in sorted_memory_cats:
                    stats = memory_stats[memory_cat]
                    # Allocated = claimed + drained
                    allocated = stats["avg_claimed"] + stats.get("avg_drained", 0.0)
                    allocated_pct = (
                        (allocated / stats["avg_total_available"] * 100) if stats["avg_total_available"] > 0 else 0
                    )
                    print(
                        f"  {memory_cat}: {allocated_pct:.1f}% "
                        f"({allocated:.1f}/{stats['avg_total_available']:.1f} GPUs)"
                    )

        print("\nBACKFILL SLOTS:")
        print(f"{'-' * 70}")
        print(f"  TOTAL: {backfill_percent:.1f}% ({backfill_claimed:.1f}/{backfill_total:.1f} GPUs)")
        print(f"{'-' * 70}")
        for class_name in backfill_slot_classes:
            if class_name in grand_totals:
                totals = grand_totals[class_name]
                print(
                    f"  {get_display_name(class_name)}: {totals['percent']:.1f}% "
                    f"({totals['claimed']:.1f}/{totals['total']:.1f} GPUs)"
                )

        # H200 Usage by Slot Type
        if "h200_user_stats" in results:
            h200_stats = results["h200_user_stats"]
            if h200_stats:
                print("\nH200 USAGE BY SLOT TYPE:")
                print(f"{'-' * 80}")

                # Aggregate data by slot type (same logic as HTML)
                slot_type_totals = {}
                slot_type_users = {}

                for user, user_data in h200_stats.items():
                    for slot_type, slot_data in user_data["slot_breakdown"].items():
                        if slot_type not in slot_type_totals:
                            slot_type_totals[slot_type] = 0
                            slot_type_users[slot_type] = []

                        slot_type_totals[slot_type] += slot_data["gpu_hours"]
                        slot_type_users[slot_type].append({"user": user, "gpu_hours": slot_data["gpu_hours"]})

                # Sort slot types by total GPU hours (descending)
                sorted_slot_types = sorted(slot_type_totals.items(), key=lambda x: x[1], reverse=True)
                total_gpu_hours = sum(slot_type_totals.values())

                # Unified format: slot type totals with user breakdown beneath
                for slot_type, total_hours in sorted_slot_types:
                    display_name = get_display_name(slot_type)
                    user_count = len(slot_type_users[slot_type])
                    percentage = (total_hours / total_gpu_hours * 100) if total_gpu_hours > 0 else 0

                    print(f"\n  {display_name} ({user_count} users): {total_hours:.1f} GPU-hours ({percentage:.1f}%)")
                    print(f"  {'-' * 60}")

                    # User breakdown beneath the slot type total
                    users = slot_type_users[slot_type]
                    users.sort(key=lambda x: x["gpu_hours"], reverse=True)

                    for user_info in users:
                        user = user_info["user"]
                        gpu_hours = user_info["gpu_hours"]
                        user_total_percentage = (gpu_hours / total_gpu_hours * 100) if total_gpu_hours > 0 else 0
                        print(f"    {user}: {gpu_hours:.1f} hrs ({user_total_percentage:.1f}%)")

        # Backfill Usage by Slot Type
        if "backfill_user_stats" in results:
            backfill_stats = results["backfill_user_stats"]
            if backfill_stats:
                print("\nBACKFILL USAGE BY SLOT TYPE (filtered):")
                print(f"{'-' * 80}")

                # Aggregate data by slot type (same logic as HTML)
                slot_type_totals = {}
                slot_type_users = {}

                for user, user_data in backfill_stats.items():
                    for slot_type, slot_data in user_data["slot_breakdown"].items():
                        if slot_type not in slot_type_totals:
                            slot_type_totals[slot_type] = 0
                            slot_type_users[slot_type] = []

                        slot_type_totals[slot_type] += slot_data["gpu_hours"]
                        slot_type_users[slot_type].append({"user": user, "gpu_hours": slot_data["gpu_hours"]})

                # Sort slot types by total GPU hours (descending)
                sorted_slot_types = sorted(slot_type_totals.items(), key=lambda x: x[1], reverse=True)
                total_gpu_hours = sum(slot_type_totals.values())

                # Unified format: slot type totals with user breakdown beneath
                for slot_type, total_hours in sorted_slot_types:
                    display_name = get_display_name(slot_type)
                    user_count = len(slot_type_users[slot_type])
                    percentage = (total_hours / total_gpu_hours * 100) if total_gpu_hours > 0 else 0

                    print(f"\n  {display_name} ({user_count} users): {total_hours:.1f} GPU-hours ({percentage:.1f}%)")
                    print(f"  {'-' * 60}")

                    # User breakdown beneath the slot type total
                    users = slot_type_users[slot_type]
                    users.sort(key=lambda x: x["gpu_hours"], reverse=True)
                    for user_info in users:
                        user = user_info["user"]
                        gpu_hours = user_info["gpu_hours"]
                        user_total_percentage = (gpu_hours / total_gpu_hours * 100) if total_gpu_hours > 0 else 0
                        print(f"    {user}: {gpu_hours:.1f} hrs ({user_total_percentage:.1f}%)")

        # Machines with Zero Active GPUs
        if "zero_active_machines" in results:
            zero_active_data = results["zero_active_machines"]
            if zero_active_data and zero_active_data["machines"]:
                print("\nMACHINES WITH ZERO ACTIVE GPUs:")
                print(f"{'-' * 120}")
                print(
                    f"Total machines: {zero_active_data['summary']['total_machines']} | Total idle GPUs: {zero_active_data['summary']['total_gpus_idle']}"
                )
                print(f"{'-' * 120}")

                # Table header
                print(f"{'Machine':<40} {'GPU Type':<25} {'GPUs':>5} {'Avg Backfill':>12} {'Prioritized Projects':<30}")
                print(f"{'-' * 120}")

                # Print all machines in a single table, sorted by machine name
                for machine_info in sorted(zero_active_data["machines"], key=lambda x: x["machine"]):
                    machine = machine_info["machine"]
                    gpu_model = machine_info["gpu_model"]
                    total_gpus = machine_info["total_gpus"]
                    avg_backfill = machine_info["avg_backfill_claimed"]
                    prioritized = machine_info["prioritized_projects"]

                    # Shorten GPU model name for better display
                    short_gpu_name = get_human_readable_device_name(gpu_model)

                    if prioritized:
                        projects_str = ", ".join(sorted(prioritized))
                        # Truncate if too long
                        if len(projects_str) > 28:
                            projects_str = projects_str[:25] + "..."
                    else:
                        projects_str = "Open Capacity"

                    print(
                        f"{machine:<40} {short_gpu_name:<25} {total_gpus:>5} {avg_backfill:>12.1f} {projects_str:<30}"
                    )

    if "allocation_stats" in results:
        print("\nAllocation Summary:")
        print(f"{'-' * 70}")
        allocation_stats = results["allocation_stats"]

        # Order with hosted capacity emphasis (enhanced format is now default)
        class_order = CLASS_ORDER

        for class_name in class_order:
            if class_name in allocation_stats:
                stats = allocation_stats[class_name]
                print(
                    f"  {get_display_name(class_name)}: {stats['allocation_usage_percent']:.1f}% "
                    f"({stats['avg_claimed']:.1f}/{stats['avg_total_available']:.1f} GPUs)"
                )

    elif "device_stats" in results:
        print("\nUsage by Device Type (filtered):")
        print(f"{'-' * 70}")

        # Use the pre-calculated grand_totals and device_stats
        class_order = CLASS_ORDER

        for class_name in class_order:
            device_data = device_stats.get(class_name, {})
            if device_data:  # Only show classes that have data
                print(f"\n{get_display_name(class_name)}:")
                print(f"{'-' * 50}")

                for device_type, stats in sorted(device_data.items()):
                    short_name = get_human_readable_device_name(device_type)

                    # Allocated = claimed + drained
                    allocated = stats["avg_claimed"] + stats.get("avg_drained", 0.0)
                    allocated_pct = (
                        (allocated / stats["avg_total_available"] * 100) if stats["avg_total_available"] > 0 else 0
                    )

                    print(
                        f"    {short_name}: {allocated_pct:.1f}% "
                        f"(avg {allocated:.1f}/{stats['avg_total_available']:.1f} GPUs)"
                    )

                # Show class total using pre-calculated data
                if class_name in grand_totals:
                    totals = grand_totals[class_name]
                    print(f"    {'-' * 30}")
                    print(
                        f"    TOTAL {get_display_name(class_name)}: {totals['percent']:.1f}% "
                        f"(avg {totals['claimed']:.1f}/{totals['total']:.1f} GPUs)"
                    )

    elif "timeseries_data" in results:
        print("\nTime Series Analysis:")
        print(f"{'-' * 70}")
        ts_df = results["timeseries_data"]

        # Calculate and display averages
        for class_name in ["priority", "shared", "backfill"]:
            usage_col = f"{class_name}_usage_percent"
            claimed_col = f"{class_name}_claimed"
            total_col = f"{class_name}_total"

            if all(col in ts_df.columns for col in [usage_col, claimed_col, total_col]):
                avg_usage = ts_df[usage_col].mean()
                avg_claimed = ts_df[claimed_col].mean()
                avg_total = ts_df[total_col].mean()
                print(f"  {class_name.title()}: {avg_usage:.1f}% ({avg_claimed:.1f}/{avg_total:.1f} GPUs)")

        # Show recent trend
        print("\nRecent Trend:")
        print(f"{'-' * 70}")
        recent_df = ts_df.tail(5)
        for _, row in recent_df.iterrows():
            print(
                f"  {row['timestamp'].strftime('%m-%d %H:%M')}: "
                f"Priority {row['priority_usage_percent']:.1f}% "
                f"({int(row['priority_claimed'])}/{int(row['priority_total'])}), "
                f"Shared {row['shared_usage_percent']:.1f}% "
                f"({int(row['shared_claimed'])}/{int(row['shared_total'])}), "
                f"Backfill {row['backfill_usage_percent']:.1f}% "
                f"({int(row['backfill_claimed'])}/{int(row['backfill_total'])})"
            )

    # Show host exclusion information at the bottom
    excluded_hosts = metadata.get("excluded_hosts", {})
    if excluded_hosts:
        print(f"\n{'=' * 70}")
        print("EXCLUDED HOSTS:")
        for host, reason in excluded_hosts.items():
            print(f"  {host}: {reason}")

    # Show filtering impact at the bottom
    filtered_info = metadata.get("filtered_hosts_info", [])
    if filtered_info:
        total_original = sum(info["original_count"] for info in filtered_info)
        total_filtered = sum(info["filtered_count"] for info in filtered_info)
        records_excluded = total_original - total_filtered
        if records_excluded > 0:
            if not excluded_hosts:  # Only print separator if excluded hosts wasn't shown
                print(f"\n{'=' * 70}")
            print("FILTERING IMPACT:")
            print(f"  Records excluded: {records_excluded:,}")
            print(f"  Records analyzed: {total_filtered:,}")

    # Add time range information at the very end
    print(f"\n{'=' * 70}")
    if metadata.get("is_monthly", False):
        # For monthly reports, show the month
        monthly_period = metadata.get("monthly_period", "Unknown Period")
        print(f"Data Period: {monthly_period}")
    else:
        # For regular reports, show start and end times
        start_time = metadata.get("start_time")
        end_time = metadata.get("end_time")
        if start_time and end_time:
            # Format timestamps nicely
            if hasattr(start_time, "strftime"):
                start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
            else:
                start_str = str(start_time)
            if hasattr(end_time, "strftime"):
                end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
            else:
                end_str = str(end_time)
            print(f"Data Period: {start_str} to {end_str}")
        else:
            # Fallback to hours_back if timestamps not available
            hours_back = metadata.get("hours_back", 24)
            print(f"Data Period: Last {hours_back} hours")
    print(f"{'=' * 70}")
