---
id: task-2.1
title: Update the table printing to include an html printing option
status: Done
assignee: []
created_date: '2025-07-08'
updated_date: '2025-07-08'
labels: []
dependencies: []
parent_task_id: task-2
---

## Description

Extend the existing print_analysis_results function to support HTML output format. Create professional-looking HTML reports with embedded charts and responsive design for email delivery.

## Description

## Acceptance Criteria

- [x] HTML output option added to usage_stats.py
- [x] HTML reports maintain same information as text reports
- [x] Charts and visualizations are embedded as images or SVG
- [x] HTML is properly formatted for email clients
- [x] Responsive design works on mobile devices
- [x] CSS styling creates professional appearance

## Implementation Notes

## Implementation Summary

Successfully implemented HTML report generation for GPU utilization reports with the following key features:

### New CLI Options
- : Choose between 'text' (default) or 'html' output
- : Optional file path to save HTML reports

### HTML Template System
- Created comprehensive HTML template in 
- Responsive design with CSS Grid and Flexbox for mobile compatibility
- Professional styling with gradient headers and color-coded utilization percentages
- Email-client compatible HTML structure

### Chart Integration
- Matplotlib charts embedded as base64-encoded PNG images
- Automatic chart generation showing utilization by GPU class
- Color-coded bars with percentage labels
- Fallback handling when matplotlib is not available

### Report Features
- Complete information parity with text reports
- Device-grouped utilization tables with hover effects
- Excluded hosts section with reasons
- Filtering impact statistics
- Cluster summary with color-coded percentages
- Mobile-responsive design with media queries

### Error Handling
- Graceful fallback to stdout if file writing fails
- Template loading error handling
- Chart generation error handling with warnings

### Dependencies Added
- jinja2 for HTML templating
- matplotlib for chart generation
- PyYAML for configuration files

### Files Modified/Created
- : Added HTML generation functions and CLI options
- : Professional HTML template
- : Added required dependencies

The implementation maintains full backward compatibility while adding powerful HTML reporting capabilities perfect for email delivery.
