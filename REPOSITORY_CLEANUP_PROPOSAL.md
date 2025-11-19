# GPU Health Monitoring Repository Cleanup Proposal

## Current State Analysis

The repository has accumulated significant technical debt with:
- 13 Python files in the root directory (many appear to be one-off experiments)
- 4 HTML files (test outputs)
- 5 analysis/results markdown files from optimization work
- Multiple test output directories (test_original_output, test_priority_output, etc.)
- Large directories: website_generator (24MB), gpu-backfill-evictions (912KB), plots (1.4MB)
- Mix of production code, debugging scripts, experiments, and temporary artifacts

## Production Workloads

Based on your usage, these are the **core production components**:

1. **Daily automated reporting**: `usage_stats.py` (sends nightly email)
2. **Periodic analysis**: `scripts/analyze_evictions.py`
3. **Data collection**: `get_gpu_state.py`
4. **Core library**: `gpu_utils.py`, `device_name_mappings.py`
5. **Configuration**: `methodology.md`, `masked_hosts.yaml`, `chtc_owned`

## Cleanup Strategy

### Phase 1: Archive Old Experiments & Analysis (Immediate)

**Move to `archive/` directory:**
- Root-level experiment scripts:
  - `daily_gpu_hours_analysis.py` (one-off analysis)
  - `generate_priority_host_heatmaps.py` (experiment)
  - `gpu_timeline_heatmap.py` (superseded by `gpu_timeline_heatmap_fast.py`?)
  - `linear_trend_fix.py` (experiment)
  - `new_gpu_function.py` (experiment)
  - `profile_usage_stats.py` (performance profiling, done)
  - `analyze.py` (legacy analysis, being phased out per README)

- Analysis artifacts:
  - `CLEANUP_ANALYSIS.md`
  - `PERFORMANCE_ANALYSIS.md`
  - `PHASE1_OPTIMIZATION_RESULTS.md`
  - `PHASE2_OPTIMIZATION_RESULTS.md`
  - `PHASE3_OPTIMIZATION_RESULTS.md`
  - `PLOT_README.md`

- Test outputs:
  - `baseline.html`, `test.html`, `test_heatmap.html`, `monthly_report.html`
  - `test_original_output/`, `test_original_png/`, `test_png_output/`, `test_priority_output/`

- Entire directories (if no longer needed):
  - `full_test_website/` (780KB)
  - `debug/` → `archive/debug/`
  - `examples/` (just has empty DB)

**Result**: Clean root directory with only production code visible.

### Phase 2: Separate Analysis Repository (Optional)

**Create new repo: `gpu-cluster-analysis`**

This would house exploratory analysis, research scripts, and specialized investigations:

**Move from gpu_health_monitoring:**
- `scripts/analyze_evictions.py` → Keep as symlink or copy
- `gpu-backfill-evictions/` (912KB)
- `website_generator/` (24MB) - appears to be a standalone project
- `plots/` (1.4MB output directory)
- `analysis/` directory
- Archived experiment scripts if you want to revisit them

**Benefits:**
- Separates production monitoring from exploratory analysis
- Can have different dependency management (analysis might need pandas, matplotlib, jupyter, etc.)
- Faster CI/CD for production monitoring code
- Clearer purpose for each repo

**Downsides:**
- Need to maintain two repos
- Shared utilities (`gpu_utils.py`) might need to be duplicated or published as package
- More complex if you frequently switch between production and analysis

### Phase 3: Reorganize Production Code

**Proposed structure:**
```
gpu_health_monitoring/
├── src/
│   ├── core/
│   │   ├── gpu_utils.py
│   │   ├── device_name_mappings.py
│   │   └── methodology.md
│   ├── collection/
│   │   └── get_gpu_state.py
│   ├── reporting/
│   │   └── usage_stats.py
│   └── analysis/
│       └── analyze_evictions.py (from scripts/)
├── config/
│   ├── masked_hosts.yaml
│   └── chtc_owned
├── scripts/
│   ├── plot_usage_stats.py
│   ├── plot_wait_times.py
│   └── (other production plotting scripts)
├── tests/
├── backlog/
├── data/           # .gitignore'd database files
├── archive/        # Old experiments and analysis
├── README.md
├── CLAUDE.md
├── pyproject.toml
└── .gitignore
```

**OR keep it simpler (recommended for your use case):**
```
gpu_health_monitoring/
├── gpu_utils.py              # Core library
├── device_name_mappings.py   # Core library
├── get_gpu_state.py          # Data collection
├── usage_stats.py            # Main reporting
├── methodology.md
├── masked_hosts.yaml
├── chtc_owned
├── scripts/                  # Analysis & plotting
│   ├── analyze_evictions.py
│   ├── plot_usage_stats.py
│   └── ...
├── tests/
├── backlog/
├── archive/                  # Moved experiments/analysis here
│   ├── experiments/
│   ├── debug/
│   ├── analysis_docs/
│   └── test_outputs/
├── templates/
├── images/
├── README.md
├── CLAUDE.md
└── pyproject.toml
```

## Recommendations

### Recommended Approach (Low Risk, High Impact):

**Do Phase 1 immediately:**
1. Create `archive/` directory with subdirectories:
   - `archive/experiments/` - one-off Python scripts
   - `archive/debug/` - debugging scripts
   - `archive/analysis_docs/` - optimization markdown files
   - `archive/test_outputs/` - HTML and test directories
2. Move non-production files to archive
3. Update `.gitignore` to ignore `*.html` in root and `test_*` directories
4. Clean commit with message like "Archive experimental code and analysis artifacts"

**Skip Phase 2 for now:**
- Keep everything in one repo unless the analysis work becomes a separate project
- `scripts/analyze_evictions.py` is fine where it is

**Optionally do minimal Phase 3:**
- Keep flat structure (it works well for your workflow)
- Main files stay in root for easy access
- Just ensure `archive/` is clearly separated

### Alternative: Separate Analysis Repository

**Only do this if:**
- You plan to do lots of exploratory GPU cluster analysis
- You want to share analysis tools with others
- The analysis work has different audiences/stakeholders than production monitoring
- You want to experiment without affecting production code

**If yes, move to separate repo:**
- `website_generator/` (clearly a separate project)
- `gpu-backfill-evictions/` (specialized analysis)
- Future exploratory notebooks and analysis scripts

## Next Steps

1. Review this proposal
2. Decide: archive-only or also separate analysis repo?
3. I can execute the cleanup with the approach you choose
4. Update README.md to reflect new structure
5. Create `.gitignore` entries for build artifacts

## Files to Definitely Keep (Production)

✅ **Core Production:**
- `gpu_utils.py`
- `device_name_mappings.py`
- `usage_stats.py`
- `get_gpu_state.py`
- `run_tests.py`
- `methodology.md`
- `masked_hosts.yaml`
- `chtc_owned`
- `README.md`
- `CLAUDE.md`

✅ **Scripts (used regularly):**
- `scripts/analyze_evictions.py`
- `scripts/plot_usage_stats.py`
- Others you actively use

✅ **Infrastructure:**
- `tests/`
- `backlog/` (task management)
- `templates/`
- `images/`
- `.claude/`, `.devcontainer/`

## Files to Archive or Delete

📦 **Archive (might reference later):**
- Experiment scripts: `daily_gpu_hours_analysis.py`, `generate_priority_host_heatmaps.py`, etc.
- Old analysis: `analyze.py`
- Performance docs: `PERFORMANCE_ANALYSIS.md`, `PHASE*_RESULTS.md`
- Debug scripts: `debug/` directory
- Test outputs: HTML files, test_* directories

❌ **Delete (can recreate if needed):**
- `examples/` (empty DB file)
- `backfill/` (empty directory)
- Old test outputs if no longer needed

Would you like me to proceed with Phase 1 (archiving), or would you prefer to discuss separating analysis into another repo first?
