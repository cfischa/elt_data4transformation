# 🔐 Airflow Credentials Setup for Destatis DAG

## Problem Identified
The `fetch_destatis_metadata` DAG is failing with **401 Unauthorized** errors because Destatis API credentials are not available in the Airflow environment.

## ✅ Solution: Configure Destatis Credentials in Airflow

The DAG has been updated to automatically look for credentials in this order:

1. **Airflow Variables** (Recommended for production)
2. **Environment Variables** (Fallback)

## 🔧 Setup Instructions

### Option 1: Airflow Variables (Recommended)

Set these variables in your Airflow UI (Admin > Variables):

#### For API Token Authentication:
```
Variable Name: DESTATIS_API_KEY
Variable Value: your_destatis_api_token_here
```

OR

```
Variable Name: DESTATIS_TOKEN  
Variable Value: your_destatis_api_token_here
```

#### For Username/Password Authentication:
```
Variable Name: DESTATIS_USER
Variable Value: your_destatis_username

Variable Name: DESTATIS_PASS
Variable Value: your_destatis_password
```

### Option 2: Environment Variables

Set these in your Airflow environment:

```bash
# For API Token
export DESTATIS_API_KEY="your_destatis_api_token_here"
# OR
export DESTATIS_TOKEN="your_destatis_api_token_here"

# For Username/Password  
export DESTATIS_USER="your_destatis_username"
export DESTATIS_PASS="your_destatis_password"
```

## 🎯 Next Steps

1. **Set the credentials** using one of the methods above
2. **Restart your Airflow scheduler** if using environment variables
3. **Re-run the DAG** - it should now authenticate successfully

## ✅ How to Verify

The DAG will now log which credential method it's using:
- `✅ Using Destatis API token from Airflow Variable`
- `✅ Using Destatis username/password from Airflow Variables`
- `✅ Using Destatis API token from environment variables`
- `✅ Using Destatis username/password from environment variables`

If no credentials are found, you'll see:
- `❌ No Destatis credentials found!` with setup instructions

## 🔒 Security Notes

- **Airflow Variables** are preferred as they can be encrypted
- Mark sensitive variables as "sensitive" in Airflow UI
- API tokens are recommended over username/password
- Never commit credentials to version control

---

## 🚀 Once Credentials Are Set

The DAG will:
1. ✅ Authenticate with Destatis API
2. ✅ Fetch all available cube metadata (up to 50,000)
3. ✅ Store directly in ClickHouse `raw.destatis_metadata`
4. ✅ Validate data insertion
5. ✅ Run weekly automatically
