---
id: task-2
title: Create a nightly email report
status: Done
assignee: []
created_date: '2025-07-08'
updated_date: '2025-07-18'
labels: []
dependencies: []
---

## Description

Create an automated nightly email report system that generates and sends GPU utilization reports to stakeholders. This involves extending the existing usage_stats.py functionality to generate HTML reports, setting up email delivery, and configuring the automation pipeline.

## Description

## Acceptance Criteria

- [ ] Nightly email reports are automatically generated and sent to stakeholders
- [ ] HTML-formatted reports include utilization summaries and device breakdowns
- [ ] Reports include charts and visualizations for better readability
- [ ] Email delivery system handles failures gracefully with retry logic
- [ ] Reports are generated using existing masked_hosts.yaml configuration
- [ ] System can be configured with recipient lists and email templates
- [ ] Reports include time period coverage and data quality metrics

## Implementation Notes

## Research Findings

### Current System Architecture
- **Data Collection**: get_gpu_state.py runs every 15 minutes via cron on remote host
- **Data Storage**: SQLite databases (gpu_state_YYYY-MM.db) stored in /home/iaross/gpureports/
- **Analysis**: usage_stats.py provides comprehensive GPU utilization analysis with device grouping
- **Visualization**: plot_usage_stats.py creates matplotlib charts for time series analysis
- **Configuration**: masked_hosts.yaml already exists for host exclusions

### Current Report Format
- Text-based reports with device-grouped utilization stats
- Includes Priority, Shared, and Backfill GPU classes
- Shows percentage utilization and average GPU counts
- Displays excluded hosts and filtering impact
- Grand totals and cluster summary sections

### Technical Implementation Plan
1. **HTML Report Generation (Task 2.1)**
   - Extend print_analysis_results() to support HTML output format
   - Create HTML templates with CSS styling for professional appearance
   - Integrate matplotlib charts as embedded images or SVG
   - Add responsive design for mobile viewing

2. **Email System (Task 2.2)**
   - SMTP configuration for email delivery
   - HTML email templates with embedded charts
   - Recipient management system
   - Error handling and retry logic
   - Dependency: Task 2.3 (cron job location)

3. **Automation Pipeline (Task 2.3)**
   - Move existing cron job to appropriate server
   - Add nightly report generation cron job
   - Configuration for report timing and recipients
   - Log monitoring and alerting for failures

### Dependencies & Libraries Needed
- Add to pyproject.toml: matplotlib, jinja2, smtplib (built-in), email (built-in)
- Consider yagmail or similar for simplified email sending
- HTML templating with Jinja2 for flexible report formatting

### Key Considerations
- Report timing should avoid peak usage periods
- Email size limits (consider hosting images externally)
- Error handling for database connectivity issues
- Configuration management for different environments
- Security considerations for email credentials

## Research Findings

### Current System Architecture
- **Data Collection**: get_gpu_state.py runs every 15 minutes via cron on remote host
- **Data Storage**: SQLite databases (gpu_state_YYYY-MM.db) stored in /home/iaross/gpureports/
- **Analysis**: usage_stats.py provides comprehensive GPU utilization analysis with device grouping
- **Visualization**: plot_usage_stats.py creates matplotlib charts for time series analysis
- **Configuration**: masked_hosts.yaml already exists for host exclusions

### Current Report Format
- Text-based reports with device-grouped utilization stats
- Includes Priority, Shared, and Backfill GPU classes
- Shows percentage utilization and average GPU counts
- Displays excluded hosts and filtering impact
- Grand totals and cluster summary sections

### Technical Implementation Plan
1. **HTML Report Generation (Task 2.1)**
   - Extend print_analysis_results() to support HTML output format
   - Create HTML templates with CSS styling for professional appearance
   - Integrate matplotlib charts as embedded images or SVG
   - Add responsive design for mobile viewing

2. **Email System (Task 2.2)** - INFRASTRUCTURE TASK (handled by iross)
   - SMTP configuration for email delivery
   - HTML email templates with embedded charts
   - Recipient management system
   - Error handling and retry logic
   - Dependency: Task 2.3 (cron job location)

3. **Automation Pipeline (Task 2.3)** - INFRASTRUCTURE TASK (handled by iross)
   - Move existing cron job to appropriate server
   - Add nightly report generation cron job
   - Configuration for report timing and recipients
   - Log monitoring and alerting for failures

### Dependencies & Libraries Needed
- Add to pyproject.toml: matplotlib, jinja2, smtplib (built-in), email (built-in)
- Consider yagmail or similar for simplified email sending
- HTML templating with Jinja2 for flexible report formatting

### Key Considerations
- Report timing should avoid peak usage periods
- Email size limits (consider hosting images externally)
- Error handling for database connectivity issues
- Configuration management for different environments
- Security considerations for email credentials

### Implementation Notes
- Tasks 2.2 and 2.3 are infrastructure tasks that will be handled by iross
- Development focus should be on Task 2.1 (HTML report generation)
- Email delivery system will piggyback on existing configuration
- HTML reports should be designed to work with existing email infrastructure
