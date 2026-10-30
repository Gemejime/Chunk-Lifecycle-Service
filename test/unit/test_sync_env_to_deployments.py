"""
Unit tests for scripts/sync_env_to_deployments.py
"""

import subprocess
import sys
import tempfile
from pathlib import Path
import unittest

# Import the script functions
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

try:
    from scripts.sync_env_to_deployments import (
        check_drift,
        get_syncable_env,
        is_sensitive,
        parse_env_file,
        sync_env,
    )
except ImportError:
    from sync_env_to_deployments import (
        check_drift,
        get_syncable_env,
        is_sensitive,
        parse_env_file,
        sync_env,
    )


class TestSyncEnvToDeployments(unittest.TestCase):
    def test_is_sensitive(self):
        # Sensitive patterns: PASSWORD, SECRET, TOKEN, KEY
        self.assertTrue(is_sensitive("PASSWORD"))
        self.assertTrue(is_sensitive("DB_PASSWORD"))
        self.assertTrue(is_sensitive("DATABASE_PASSWORD"))
        self.assertTrue(is_sensitive("SECRET"))
        self.assertTrue(is_sensitive("JWT_SECRET"))
        self.assertTrue(is_sensitive("TOKEN"))
        self.assertTrue(is_sensitive("AUTH_TOKEN"))
        self.assertTrue(is_sensitive("KEY"))
        self.assertTrue(is_sensitive("API_KEY"))
        self.assertTrue(is_sensitive("PRIVATE_KEY"))

        # Non-sensitive keys (without credential values)
        self.assertFalse(is_sensitive("ENVIRONMENT"))
        self.assertFalse(is_sensitive("HTTP_PORT"))
        self.assertFalse(is_sensitive("PORT"))
        self.assertFalse(is_sensitive("GRPC_PORT"))
        self.assertFalse(is_sensitive("LOG_LEVEL"))
        self.assertFalse(is_sensitive("REDIS_ADDR"))
        self.assertFalse(is_sensitive("DATABASE_URL"))

        # Credential-bearing URL values
        self.assertTrue(is_sensitive("DATABASE_URL", "postgresql://user:pass@host/db"))
        self.assertTrue(is_sensitive("REDIS_ADDR", "rediss://default:secret@host:6379"))
        self.assertTrue(is_sensitive("APP_URL", "https://admin:token123@api.internal"))
        self.assertFalse(is_sensitive("REDIS_ADDR", "redis-cluster.cache.svc.cluster.local:6379"))
        self.assertFalse(is_sensitive("HTTP_URL", "http://prometheus:9090"))

    def test_parse_env_file(self):
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=False) as f:
            f.write("""
# Sample Comment
ENVIRONMENT=production
HTTP_PORT="8080"
GRPC_PORT='50051'
LOG_LEVEL=info # inline comment
DATABASE_PASSWORD=supersecret
EMPTY_VAL=
""")
            temp_path = Path(f.name)

        try:
            env = parse_env_file(temp_path)
            self.assertEqual(env["ENVIRONMENT"], "production")
            self.assertEqual(env["HTTP_PORT"], "8080")
            self.assertEqual(env["GRPC_PORT"], "50051")
            self.assertEqual(env["LOG_LEVEL"], "info")
            self.assertEqual(env["DATABASE_PASSWORD"], "supersecret")
            self.assertEqual(env["EMPTY_VAL"], "")
            self.assertNotIn("# Sample Comment", env)
        finally:
            temp_path.unlink(missing_ok=True)

    def test_get_syncable_env_excludes_credentials(self):
        env = {
            "ENVIRONMENT": "production",
            "HTTP_PORT": "8080",
            "DB_PASSWORD": "secret_password",
            "AUTH_TOKEN": "secret_token",
            "API_KEY": "secret_key",
            "OAUTH_SECRET": "secret_oauth",
            "DATABASE_URL": "postgresql://neondb_owner:mypassword@ep-host.tech/db",
            "REDIS_ADDR": "redis-cluster.cache.svc.cluster.local:6379",
        }
        syncable = get_syncable_env(env)
        self.assertIn("ENVIRONMENT", syncable)
        self.assertIn("HTTP_PORT", syncable)
        self.assertIn("REDIS_ADDR", syncable)
        self.assertNotIn("DB_PASSWORD", syncable)
        self.assertNotIn("AUTH_TOKEN", syncable)
        self.assertNotIn("API_KEY", syncable)
        self.assertNotIn("OAUTH_SECRET", syncable)
        self.assertNotIn("DATABASE_URL", syncable)

    def test_check_drift_sanitized_output(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            env_file = tmp_path / ".env.prod"
            values_file = tmp_path / "values.yaml"

            env_file.write_text("ENVIRONMENT=production\nHTTP_PORT=8080\n", encoding="utf-8")
            values_file.write_text("env:\n  ENVIRONMENT: staging\n  HTTP_PORT: '8080'\n", encoding="utf-8")

            # Capture stderr
            import io
            from contextlib import redirect_stderr
            f_err = io.StringIO()
            with redirect_stderr(f_err):
                ret = check_drift(env_file, values_file)
            self.assertEqual(ret, 1)
            output = f_err.getvalue()
            self.assertIn("ENVIRONMENT: value mismatch detected", output)
            self.assertNotIn("ENVIRONMENT='production'", output)
            self.assertNotIn("ENVIRONMENT='staging'", output)

    def test_sync_preserves_secret_name_and_blocks(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            env_file = tmp_path / ".env.prod"
            values_file = tmp_path / "values.yaml"

            env_file.write_text("""
ENVIRONMENT=production
HTTP_PORT=8080
GRPC_PORT=50051
DB_PASSWORD=should_be_excluded
REDIS_ADDR=redis:6379
""", encoding="utf-8")

            values_file.write_text("""# Header comment
image:
  repository: myrepo
  tag: "1.0.0"

env:
  secretName: "existing-app-secret"
  OLD_VAR: "old_val"

resources:
  cpu: 500m
""", encoding="utf-8")

            # 1. Before sync, check_drift should detect drift
            self.assertEqual(check_drift(env_file, values_file), 1)

            # 2. Perform sync
            self.assertEqual(sync_env(env_file, values_file), 0)

            # 3. After sync, check_drift should return 0 (zero drift)
            self.assertEqual(check_drift(env_file, values_file), 0)

            # 4. Verify content preservation
            synced_content = values_file.read_text(encoding="utf-8")
            self.assertIn("# Header comment", synced_content)
            self.assertIn("repository: myrepo", synced_content)
            self.assertIn("cpu: 500m", synced_content)
            self.assertIn('secretName: "existing-app-secret"', synced_content)
            self.assertIn('ENVIRONMENT: "production"', synced_content)
            self.assertIn('HTTP_PORT: "8080"', synced_content)
            self.assertIn('GRPC_PORT: "50051"', synced_content)
            self.assertIn('REDIS_ADDR: "redis:6379"', synced_content)
            # Ensure DB_PASSWORD was never written
            self.assertNotIn("should_be_excluded", synced_content)
            self.assertNotIn("DB_PASSWORD", synced_content)
            # Ensure OLD_VAR was pruned
            self.assertNotIn("OLD_VAR", synced_content)

    def test_cli_flags(self):
        script_path = REPO_ROOT / "scripts" / "sync_env_to_deployments.py"
        # Test --check-only on repo
        res = subprocess.run(
            [sys.executable, str(script_path), "--check-only"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        self.assertEqual(res.returncode, 0, f"stdout: {res.stdout}\nstderr: {res.stderr}")
        self.assertIn("[OK] Zero drift", res.stdout)


if __name__ == "__main__":
    unittest.main()

