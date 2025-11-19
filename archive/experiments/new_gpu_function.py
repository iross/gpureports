def get_gpu_hours_from_db(db_path):
    """Extract GPU usage data from a single database file, separated by slot type."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Query to get claimed primary slots with timestamps
        primary_query = """
        SELECT
            timestamp,
            COUNT(*) as claimed_gpus
        FROM gpu_state
        WHERE State = 'Claimed' AND Name LIKE 'slot1_%'
        GROUP BY timestamp
        ORDER BY timestamp
        """

        # Query to get claimed backfill slots with timestamps
        backfill_query = """
        SELECT
            timestamp,
            COUNT(*) as claimed_gpus
        FROM gpu_state
        WHERE State = 'Claimed' AND Name LIKE 'backfill2_%'
        GROUP BY timestamp
        ORDER BY timestamp
        """

        cursor.execute(primary_query)
        primary_results = cursor.fetchall()

        cursor.execute(backfill_query)
        backfill_results = cursor.fetchall()

        conn.close()

        if not primary_results and not backfill_results:
            print(f"Warning: No claimed GPU data found in {db_path}")
            return {}

        # Calculate GPU hours for both slot types
        daily_data = defaultdict(lambda: {
            'primary_gpu_hours': 0, 'primary_claimed_gpus': 0, 'primary_measurements': 0,
            'backfill_gpu_hours': 0, 'backfill_claimed_gpus': 0, 'backfill_measurements': 0
        })

        # Process primary slots
        for i in range(len(primary_results)):
            timestamp_str, claimed_gpus = primary_results[i]
            current_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            date_str = current_time.date().isoformat()

            # Calculate time interval for this measurement
            if i < len(primary_results) - 1:
                next_timestamp_str = primary_results[i + 1][0]
                next_time = datetime.fromisoformat(next_timestamp_str.replace('Z', '+00:00'))
                interval_hours = (next_time - current_time).total_seconds() / 3600
            else:
                if i > 0:
                    prev_timestamp_str = primary_results[i - 1][0]
                    prev_time = datetime.fromisoformat(prev_timestamp_str.replace('Z', '+00:00'))
                    interval_hours = (current_time - prev_time).total_seconds() / 3600
                else:
                    interval_hours = 0.25

            gpu_hours = claimed_gpus * interval_hours
            daily_data[date_str]['primary_gpu_hours'] += gpu_hours
            daily_data[date_str]['primary_claimed_gpus'] += claimed_gpus
            daily_data[date_str]['primary_measurements'] += 1

        # Process backfill slots
        for i in range(len(backfill_results)):
            timestamp_str, claimed_gpus = backfill_results[i]
            current_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            date_str = current_time.date().isoformat()

            # Calculate time interval for this measurement
            if i < len(backfill_results) - 1:
                next_timestamp_str = backfill_results[i + 1][0]
                next_time = datetime.fromisoformat(next_timestamp_str.replace('Z', '+00:00'))
                interval_hours = (next_time - current_time).total_seconds() / 3600
            else:
                if i > 0:
                    prev_timestamp_str = backfill_results[i - 1][0]
                    prev_time = datetime.fromisoformat(prev_timestamp_str.replace('Z', '+00:00'))
                    interval_hours = (current_time - prev_time).total_seconds() / 3600
                else:
                    interval_hours = 0.25

            gpu_hours = claimed_gpus * interval_hours
            daily_data[date_str]['backfill_gpu_hours'] += gpu_hours
            daily_data[date_str]['backfill_claimed_gpus'] += claimed_gpus
            daily_data[date_str]['backfill_measurements'] += 1

        # Convert to final format with totals
        data = {}
        for date_str, metrics in daily_data.items():
            # Calculate averages
            avg_primary_gpus = (metrics['primary_claimed_gpus'] / metrics['primary_measurements']
                              if metrics['primary_measurements'] > 0 else 0)
            avg_backfill_gpus = (metrics['backfill_claimed_gpus'] / metrics['backfill_measurements']
                               if metrics['backfill_measurements'] > 0 else 0)

            total_gpu_hours = metrics['primary_gpu_hours'] + metrics['backfill_gpu_hours']
            total_avg_gpus = avg_primary_gpus + avg_backfill_gpus

            data[date_str] = {
                'claimed_gpus': total_avg_gpus,
                'gpu_hours': total_gpu_hours,
                'primary_gpu_hours': metrics['primary_gpu_hours'],
                'primary_claimed_gpus': avg_primary_gpus,
                'backfill_gpu_hours': metrics['backfill_gpu_hours'],
                'backfill_claimed_gpus': avg_backfill_gpus
            }

        return data
    except Exception as e:
        print(f"Error processing {db_path}: {e}")
        return {}
