#!/usr/bin/env python3
"""
Check for flagship and standard tier GPUs that have been unused in the last week.
"""

import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import yaml

# Flagship GPU patterns
FLAGSHIP_PATTERNS = ["H100", "H200", "A100-SXM4-80GB", "A100 80GB"]


def load_host_exclusions(yaml_file: str = "masked_hosts.yaml") -> dict[str, str]:
    """
    Load host exclusion configuration from YAML file.

    Args:
        yaml_file: Path to YAML file with exclusions

    Returns:
        Dictionary mapping excluded host patterns to reasons
    """
    exclusions = {}

    if Path(yaml_file).exists():
        try:
            with open(yaml_file) as f:
                data = yaml.safe_load(f)
                if data and "excluded_hosts" in data:
                    exclusions = data["excluded_hosts"]
        except Exception as e:
            print(f"Warning: Could not load exclusions from {yaml_file}: {e}")

    return exclusions


def is_excluded_host(machine: str, exclusions: dict[str, str]) -> bool:
    """
    Check if a machine is in the exclusion list (case-insensitive).

    Args:
        machine: Machine hostname
        exclusions: Dictionary of excluded host patterns

    Returns:
        True if machine should be excluded
    """
    for excluded_pattern in exclusions.keys():
        if re.search(excluded_pattern, machine, re.IGNORECASE):
            return True
    return False


def get_gpu_tier(device_name):
    """Get GPU tier (Flagship or Standard)."""
    if not device_name:
        return "Unknown"
    for pattern in FLAGSHIP_PATTERNS:
        if pattern in device_name:
            return "Flagship"
    return "Standard"


def main():
    # Connect to the current month's database
    db_path = "gpu_state_2025-12.db"

    # Load host exclusions
    exclusions = load_host_exclusions()
    if exclusions:
        print("Loaded host exclusions:")
        for host, reason in exclusions.items():
            print(f"  - {host}: {reason}")
        print()

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        RANGE = 1
        # Get date range (last 7 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=RANGE)

        print(f"Analyzing GPU usage from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print("=" * 80)

        # Query to get all GPU records in the last week
        query = """
        SELECT
            Machine,
            GPUs_DeviceName,
            State,
            AssignedGPUs,
            GPUsAverageUsage,
            PrioritizedProjects,
            timestamp
        FROM gpu_state
        WHERE timestamp >= ?
        AND timestamp <= ?
        AND GPUs_DeviceName IS NOT NULL
        AND AssignedGPUs IS NOT NULL
        ORDER BY Machine, timestamp
        """

        cursor.execute(query, (start_date.isoformat(), end_date.isoformat()))
        records = cursor.fetchall()

        # Track usage per individual GPU
        # Key: (machine, gpu_id), Value: {tier, device_name, total_records, claimed_records, prioritized_projects}
        gpu_usage = defaultdict(
            lambda: {
                "tier": None,
                "device_name": None,
                "total_records": 0,
                "claimed_records": 0,
                "prioritized_projects": set(),
            }
        )

        # Also track all GPUs per machine to count totals
        machine_gpus = defaultdict(set)  # machine -> set of gpu_ids

        for machine, device_name, state, assigned_gpu, _avg_usage, prioritized_projects, _timestamp in records:
            # Skip excluded hosts
            if is_excluded_host(machine, exclusions):
                continue

            tier = get_gpu_tier(device_name)
            gpu_id = assigned_gpu.strip() if assigned_gpu else None

            if not gpu_id:
                continue

            key = (machine, gpu_id)
            machine_gpus[machine].add(gpu_id)

            gpu_usage[key]["tier"] = tier
            gpu_usage[key]["device_name"] = device_name
            gpu_usage[key]["total_records"] += 1

            # Track prioritized projects (if any)
            if prioritized_projects and prioritized_projects.strip():
                gpu_usage[key]["prioritized_projects"].add(prioritized_projects.strip())

            if state == "Claimed":
                gpu_usage[key]["claimed_records"] += 1

        # Find completely unused GPUs (never claimed) by tier
        unused_by_tier = {"Flagship": [], "Standard": []}

        for (machine, gpu_id), stats in gpu_usage.items():
            if stats["claimed_records"] == 0:
                unused_by_tier[stats["tier"]].append(
                    {
                        "machine": machine,
                        "gpu_id": gpu_id,
                        "device_name": stats["device_name"],
                        "total_records": stats["total_records"],
                        "prioritized_projects": stats["prioritized_projects"],
                    }
                )

        # Count unused GPUs per machine
        unused_count_by_machine = defaultdict(lambda: {"flagship": 0, "standard": 0, "device_name": None})

        for tier in ["Flagship", "Standard"]:
            for gpu in unused_by_tier[tier]:
                machine = gpu["machine"]
                unused_count_by_machine[machine]["device_name"] = gpu["device_name"]
                if tier == "Flagship":
                    unused_count_by_machine[machine]["flagship"] += 1
                else:
                    unused_count_by_machine[machine]["standard"] += 1

        # Print results for each tier
        for tier in ["Flagship", "Standard"]:
            unused_gpus = unused_by_tier[tier]

            if unused_gpus:
                print(f"\n{'='*80}")
                print(
                    f"{tier.upper()} TIER - {len(unused_gpus)} Primary slot GPU(s) NEVER claimed in the last {RANGE} days"
                )
                print(f"{'='*80}")

                # Group by device type
                by_device = defaultdict(list)
                for gpu in unused_gpus:
                    by_device[gpu["device_name"]].append(gpu)

                for device_name in sorted(by_device.keys()):
                    gpus = by_device[device_name]

                    # Group by machine to count GPUs per host
                    by_machine = defaultdict(list)
                    for gpu in gpus:
                        by_machine[gpu["machine"]].append(gpu)

                    print(f"\n{device_name} ({len(by_machine)} host(s), {len(gpus)} GPU(s)):")
                    print("-" * 80)

                    for machine in sorted(by_machine.keys()):
                        gpu_list = by_machine[machine]
                        total_gpus_on_host = len(machine_gpus[machine])
                        unused_on_host = len(gpu_list)

                        # Get prioritized projects (should be same for all GPUs on a machine)
                        all_projects = set()
                        for gpu in gpu_list:
                            all_projects.update(gpu["prioritized_projects"])

                        print(f"  • {machine}")
                        print(f"    - Unused GPUs: {unused_on_host} of {total_gpus_on_host} total")

                        if all_projects:
                            projects_str = ", ".join(sorted(all_projects))
                            print(f"    - Prioritized for: {projects_str}")
                        else:
                            print("    - Prioritized for: None (Open Capacity)")

                        print(f"    - GPU IDs: {', '.join([g['gpu_id'] for g in gpu_list])}")
            else:
                print(f"\n✓ All {tier} tier GPUs have been claimed at least once in the last 7 days!")

        # Summary statistics
        print("\n" + "=" * 80)
        print("SUMMARY: GPU Usage Statistics (Last 7 Days)")
        print("=" * 80)

        flagship_gpus = [(k, v) for k, v in gpu_usage.items() if v["tier"] == "Flagship"]
        standard_gpus = [(k, v) for k, v in gpu_usage.items() if v["tier"] == "Standard"]

        flagship_unused = len(unused_by_tier["Flagship"])
        standard_unused = len(unused_by_tier["Standard"])

        print("\nFlagship Tier:")
        print(f"  Total GPUs tracked: {len(flagship_gpus)}")
        print(f"  Never claimed: {flagship_unused}")
        print(f"  Claimed at least once: {len(flagship_gpus) - flagship_unused}")
        if len(flagship_gpus) > 0:
            print(f"  Utilization rate: {((len(flagship_gpus) - flagship_unused) / len(flagship_gpus) * 100):.1f}%")

        print("\nStandard Tier:")
        print(f"  Total GPUs tracked: {len(standard_gpus)}")
        print(f"  Never claimed: {standard_unused}")
        print(f"  Claimed at least once: {len(standard_gpus) - standard_unused}")
        if len(standard_gpus) > 0:
            print(f"  Utilization rate: {((len(standard_gpus) - standard_unused) / len(standard_gpus) * 100):.1f}%")

        print("\nOverall:")
        total_gpus = len(gpu_usage)
        total_unused = flagship_unused + standard_unused
        print(f"  Total GPUs tracked: {total_gpus}")
        print(f"  Never claimed: {total_unused}")
        print(f"  Claimed at least once: {total_gpus - total_unused}")
        if total_gpus > 0:
            print(f"  Overall utilization rate: {((total_gpus - total_unused) / total_gpus * 100):.1f}%")

        conn.close()

    except sqlite3.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
