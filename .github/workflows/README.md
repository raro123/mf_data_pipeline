# GitHub Actions Workflows

This directory contains automated workflows for the Mutual Fund Data Pipeline.

## Workflows

### `daily-nav-processing.yml`

**Purpose:** Automated daily processing of NAV (Net Asset Value) data from AMFI.

**Schedule:**
- Runs daily at 11:00 PM UTC (4:30 AM IST / 5:30 AM IST during DST)
- Can be manually triggered via the Actions tab

**Process Flow:**
1. **Setup Environment**
   - Checkout code
   - Setup Python 3.9
   - Install dependencies from `requirements.txt`
   - Create required directories

2. **Daily NAV Transform** (`03_daily_nav_transform.py`)
   - Fetches latest NAV data from AMFI API
   - Processes and stores raw data in R2 bucket
   - Handles gap-filling for missing dates

3. **Daily NAV Clean** (`daily_nav_clean.py`)
   - Only runs if transform step succeeds
   - Cleans and enriches NAV data with metadata
   - Creates analysis-ready datasets

4. **Error Handling**
   - Uploads logs as artifacts on failure
   - Sends notifications on success/failure

## Required Configuration

Before the workflow can run successfully, you must configure:

1. **GitHub Secrets** (required):
   - `R2_ACCESS_KEY_ID`
   - `R2_SECRET_ACCESS_KEY`
   - `R2_ACCOUNT_ID`

2. **GitHub Variables** (optional):
   - `AMFI_NAV_TIMEOUT`, `MAX_RETRIES`, etc.

See `GITHUB_ACTIONS_SETUP.md` for detailed setup instructions.

## Monitoring

### Viewing Workflow Status
1. Go to the **Actions** tab in your repository
2. Click on "Daily NAV Data Processing"
3. View recent runs and their status

### Manual Execution
1. In the Actions tab, select the workflow
2. Click "Run workflow" button
3. Choose branch (usually `main`)
4. Click "Run workflow"

## Troubleshooting

### Common Issues

1. **Authentication Failed**
   - Check that R2 secrets are correctly set
   - Verify R2 token permissions

2. **Script Failures**
   - Check workflow logs for specific error messages
   - Download artifact logs for detailed debugging
   - Verify AMFI API availability

3. **Timeout Issues**
   - Increase timeout values in GitHub Variables
   - Check network connectivity to AMFI API

### Getting Help

- Review workflow run logs in the Actions tab
- Check `GITHUB_ACTIONS_SETUP.md` for configuration details
- Use the test script: `python scripts/test_github_actions_setup.py`

## Best Practices

1. **Test First:** Always test manually before relying on scheduled runs
2. **Monitor Regularly:** Check workflow status weekly
3. **Keep Updated:** Update dependencies in `requirements.txt` regularly
4. **Security:** Rotate R2 API tokens quarterly