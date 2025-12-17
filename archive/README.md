# Archive Directory

This directory contains historical experiments, analysis artifacts, and test outputs that are no longer part of active development but are preserved for reference.

## Contents

### experiments/
One-off analysis scripts and experimental code:
- `analyze.py` - Legacy analysis functions (phased out, replaced by usage_stats.py)
- `daily_gpu_hours_analysis.py` - Daily GPU hours analysis experiment
- `generate_priority_host_heatmaps.py` - Priority host heatmap generation
- `gpu_timeline_heatmap.py` - Timeline heatmap visualization (superseded by gpu_timeline_heatmap_fast.py)
- `linear_trend_fix.py` - Linear trend analysis experiment
- `new_gpu_function.py` - GPU function prototype
- `profile_usage_stats.py` - Performance profiling of usage_stats.py

### debug/
Debugging and diagnostic scripts from troubleshooting sessions:
- `debug_backfill_only.py` - Backfill slot debugging
- `debug_h200*.py` - H200 GPU specific debugging
- `debug_priority_filter.py` - Priority filtering debugging

### analysis_docs/
Performance optimization and cleanup analysis documentation:
- `CLEANUP_ANALYSIS.md` - Code cleanup analysis
- `PERFORMANCE_ANALYSIS.md` - Performance optimization analysis
- `PHASE1_OPTIMIZATION_RESULTS.md` - Phase 1 optimization results
- `PHASE2_OPTIMIZATION_RESULTS.md` - Phase 2 optimization results  
- `PHASE3_OPTIMIZATION_RESULTS.md` - Phase 3 optimization results
- `PLOT_README.md` - Plotting documentation

### test_outputs/
Test HTML outputs and validation artifacts:
- `baseline.html`, `test.html`, `test_heatmap.html`, `monthly_report.html` - Test report outputs
- `test_original_output/`, `test_original_png/`, `test_png_output/`, `test_priority_output/` - Test result directories

### examples/
Example data and empty directories preserved for reference

## Note

Files in this archive are not maintained and may not work with current codebase versions. They are preserved for historical reference and to document the evolution of the project.
