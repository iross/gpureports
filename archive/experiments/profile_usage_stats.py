#!/usr/bin/env python3
"""
Performance profiling script for usage_stats.py
"""
import cProfile
import pstats
import io
import time
import sys
import usage_stats

def profile_function(func, *args, **kwargs):
    """Profile a function and return timing stats."""
    pr = cProfile.Profile()
    pr.enable()
    
    start = time.time()
    result = func(*args, **kwargs)
    end = time.time()
    
    pr.disable()
    
    # Get stats
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats()
    
    return result, end - start, s.getvalue()

def main():
    print("Detailed Performance Profiling - usage_stats.py")
    print("=" * 60)
    
    # Profile full analysis with 24 hours (typical daily report)
    print("\nProfiling 24-hour analysis (daily report scenario)...")
    
    try:
        results, duration, profile_stats = profile_function(
            usage_stats.run_analysis,
            'gpu_state_2025-08.db',
            hours_back=24,
            group_by_device=True
        )
        
        print(f"Total Duration: {duration:.2f}s")
        print(f"Records Processed: {results['metadata']['total_records']:,}")
        print(f"Processing Rate: {results['metadata']['total_records']/duration:,.0f} records/sec")
        
        # Show top 10 most time-consuming functions
        print("\\nTop 10 Performance Bottlenecks:")
        print("-" * 60)
        
        lines = profile_stats.split('\\n')
        header_found = False
        count = 0
        
        for line in lines:
            if 'cumulative' in line and 'filename:lineno(function)' in line:
                header_found = True
                print(line)
                continue
            elif header_found and line.strip() and count < 10:
                if not line.strip().startswith('ncalls'):
                    print(line)
                    count += 1
                    
        # Test specific function performance
        print("\\n" + "=" * 60)
        print("Individual Function Performance:")
        print("=" * 60)
        
        # Test database loading
        start_time = time.time()
        df = usage_stats.get_time_filtered_data('gpu_state_2025-08.db', 24)
        load_time = time.time() - start_time
        print(f"Database loading: {load_time:.2f}s ({len(df):,} records)")
        
        if len(df) > 0:
            # Test device allocation calculation
            start_time = time.time()
            device_stats = usage_stats.calculate_allocation_usage_by_device_enhanced(df, "", False)
            calc_time = time.time() - start_time
            print(f"Device allocation calc: {calc_time:.2f}s")
            
            # Test H200 user breakdown
            start_time = time.time()
            h200_stats = usage_stats.calculate_h200_user_breakdown(df, "", 24)
            h200_time = time.time() - start_time
            print(f"H200 user breakdown: {h200_time:.2f}s")
            
            # Test memory stats
            start_time = time.time() 
            memory_stats = usage_stats.calculate_allocation_usage_by_memory(df, "", False)
            memory_time = time.time() - start_time
            print(f"Memory allocation calc: {memory_time:.2f}s")
        
        print(f"\\nTotal overhead: {duration - load_time - calc_time - h200_time - memory_time:.2f}s")
        
    except Exception as e:
        print(f"Profiling failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()