# GitHub Actions Setup for Daily NAV Processing

This document explains how to configure GitHub Actions for automated daily NAV data processing.

## Overview

The GitHub Actions workflow `daily-nav-processing.yml` automatically:
1. Runs at 11:00 PM UTC daily (6:30 AM IST next day)
2. Executes `03_daily_nav_transform.py` to fetch and process daily NAV data
3. If successful, runs `daily_nav_clean.py` to clean and enrich the data
4. Uploads logs on failure for debugging

## Required Setup

### 1. GitHub Secrets (Required)

These are sensitive credentials that must be configured in your GitHub repository settings:

**Repository Settings → Secrets and variables → Actions → New repository secret**

| Secret Name | Description | Example Value |
|-------------|-------------|---------------|
| `R2_ACCESS_KEY_ID` | Cloudflare R2 Access Key ID | `abc123def456...` |
| `R2_SECRET_ACCESS_KEY` | Cloudflare R2 Secret Access Key | `xyz789uvw012...` |
| `R2_ACCOUNT_ID` | Cloudflare R2 Account ID | `a1b2c3d4e5f6...` |

**How to add secrets:**
1. Go to your GitHub repository
2. Click on **Settings** tab
3. In the left sidebar, click **Secrets and variables** → **Actions**
4. Click **New repository secret**
5. Add each secret with the exact name and value

### 2. GitHub Variables (Optional)

These are non-sensitive configuration values that can be customized:

**Repository Settings → Secrets and variables → Actions → Variables tab → New repository variable**

| Variable Name | Description | Default Value | Recommended Value |
|---------------|-------------|---------------|-------------------|
| `AMFI_NAV_TIMEOUT` | API timeout for NAV requests (seconds) | `30` | `30` |
| `AMFI_SCHEME_TIMEOUT` | API timeout for scheme requests (seconds) | `30` | `30` |
| `MAX_RETRIES` | Maximum retry attempts for failed API calls | `3` | `3` |
| `RETRY_DELAY` | Delay between retries (seconds) | `5` | `5` |
| `HISTORICAL_FETCH_DAYS` | Days of historical data to fetch | `90` | `90` |
| `CHUNK_SIZE` | Processing chunk size | `10000` | `10000` |
| `LOG_LEVEL` | Logging level | `INFO` | `INFO` |

### 3. Cloudflare R2 Setup

#### Creating R2 Credentials:

1. **Login to Cloudflare Dashboard**
2. **Navigate to R2 Object Storage**
3. **Create API Token:**
   - Go to "Manage R2 API Tokens"
   - Click "Create API Token"
   - Choose "Custom token"
   - Set permissions: `Object:Read`, `Object:Write`, `Bucket:Read`
   - Add your bucket name under "Account resources"
   - Click "Continue to summary" → "Create Token"
   - **Save the Access Key ID and Secret Access Key**

4. **Get Account ID:**
   - In Cloudflare dashboard, the Account ID is shown in the right sidebar
   - Copy this ID for the `R2_ACCOUNT_ID` secret

#### Bucket Configuration:

Ensure your R2 bucket has the following structure:
```
financial-data-store/
├── mutual_funds/
│   ├── raw/
│   │   ├── nav_historical/
│   │   └── nav_daily/
│   └── clean/
│       ├── nav_daily_growth_plan/
│       └── scheme_metadata/
```

## Workflow Configuration

### Schedule

- **Cron expression:** `0 23 * * *` (11:00 PM UTC daily)
- **IST equivalent:** 4:30 AM IST (or 5:30 AM during DST)
- **Reasoning:** Runs early morning IST to process previous day's NAV data

### Manual Triggering

You can manually trigger the workflow:
1. Go to **Actions** tab in your repository
2. Click on "Daily NAV Data Processing"
3. Click "Run workflow" button

### Error Handling

- If `03_daily_nav_transform.py` fails, the workflow stops
- If `daily_nav_clean.py` fails, logs are uploaded as artifacts
- Artifacts are retained for 7 days for debugging

## Monitoring and Troubleshooting

### Viewing Workflow Results

1. Go to **Actions** tab in your repository
2. Click on "Daily NAV Data Processing"
3. View recent workflow runs and their status

### Debugging Failed Runs

1. Click on a failed workflow run
2. Expand the failed step to see error details
3. Download log artifacts if available
4. Check the R2 bucket for any partial data

### Common Issues and Solutions

| Issue | Solution |
|-------|----------|
| Authentication errors | Verify R2 credentials in secrets |
| Timeout errors | Increase timeout values in variables |
| API rate limits | Increase retry delay or reduce frequency |
| Missing dependencies | Ensure `requirements.txt` is up to date |
| Data validation errors | Check AMFI API data format changes |

## Security Best Practices

1. **Never commit credentials** to the repository
2. **Use GitHub Secrets** for all sensitive information
3. **Regularly rotate** R2 API tokens
4. **Monitor workflow logs** for any suspicious activity
5. **Limit R2 token permissions** to minimum required access

## Testing the Setup

### Local Testing

Before relying on GitHub Actions, test locally:

```bash
# Set environment variables
export R2_ACCESS_KEY_ID="your_access_key"
export R2_SECRET_ACCESS_KEY="your_secret_key"
export R2_ACCOUNT_ID="your_account_id"

# Run scripts manually
python scripts/03_daily_nav_transform.py
python scripts/daily_nav_clean.py
```

### GitHub Actions Testing

1. **Push the workflow file** to your repository
2. **Manually trigger** the workflow first
3. **Check the results** before relying on scheduled runs
4. **Verify data** in your R2 bucket

## Maintenance

### Regular Tasks

- **Monitor workflow runs** weekly
- **Update dependencies** monthly in `requirements.txt`
- **Review and rotate** R2 API tokens quarterly
- **Check disk usage** in R2 bucket monthly

### Scaling Considerations

- For high-frequency processing, consider using GitHub Actions with more powerful runners
- For large datasets, implement data partitioning strategies
- Monitor GitHub Actions usage limits for your plan

## Support

For issues with:
- **GitHub Actions:** Check GitHub documentation or repository issues
- **R2 Storage:** Contact Cloudflare support
- **AMFI API:** Check AMFI website for API changes
- **Script errors:** Review application logs and error messages