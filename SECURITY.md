# SECURITY NOTICE

## ⚠️ Important: Change Default Credentials

This project uses **default credentials for local development only**. Before deploying or sharing:

### 1. Generate a Secure Fernet Key

The Airflow Fernet key is used to encrypt sensitive data. Generate your own:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Then update:
- `.env` file: `AIRFLOW__CORE__FERNET_KEY=your_generated_key`
- `docker-compose.yml` lines 56 and 92 with your generated key

### 2. Change Default Passwords

Update these in production:
- **PostgreSQL**: `POSTGRES_PASSWORD` (currently: `airflow`)
- **MinIO**: `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` (currently: `minioadmin/minioadmin`)
- **Airflow Admin**: `AIRFLOW_ADMIN_PASSWORD` (currently: `admin`)

### 3. Never Commit `.env` File

The `.env` file is in `.gitignore` - keep it that way! Only commit `.env.example` as a template.

### 4. For Production Deployment

Use proper secrets management:
- **AWS**: AWS Secrets Manager, Parameter Store
- **GCP**: Secret Manager
- **Azure**: Key Vault
- **Kubernetes**: Sealed Secrets, External Secrets Operator

---

## What's Safe vs Not Safe

✅ **Safe to commit**:
- `.env.example` - template with placeholders
- `docker-compose.yml` - uses default values for local dev

❌ **Never commit**:
- `.env` - your actual environment file with real keys
- Any files with production credentials
- API keys, tokens, passwords

---

**Note**: The current setup uses default credentials suitable **ONLY for local testing**. This makes it easy to get started quickly, but should **never be used in production**.
