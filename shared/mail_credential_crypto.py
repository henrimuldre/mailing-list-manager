import logging
import os


MAIL_CREDENTIALS_KEY_ENV = "MAIL_CREDENTIALS_KEY"
# Optional, comma-separated: previous keys that can still decrypt (never encrypt).
# Used to rotate the key: put the new key in MAIL_CREDENTIALS_KEY, the old one here, run scripts/migrate_mail_credentials.py --rotate, then remove the old key.
MAIL_CREDENTIALS_OLD_KEYS_ENV = "MAIL_CREDENTIALS_OLD_KEYS"
MAIL_CREDENTIALS_PREFIX = "enc:v1:"

log = logging.getLogger("mail_credentials")
_plaintext_warned = set()


def mail_credentials_key_configured():
    return bool((os.getenv(MAIL_CREDENTIALS_KEY_ENV) or "").strip())


def is_encrypted_mail_secret(value):
    return isinstance(value, str) and value.startswith(MAIL_CREDENTIALS_PREFIX)


def _build_missing_key_error(action):
    return RuntimeError(
        f"{MAIL_CREDENTIALS_KEY_ENV} must be set to {action} stored mail credentials"
    )


def _get_fernet():
    key = (os.getenv(MAIL_CREDENTIALS_KEY_ENV) or "").strip()
    if not key:
        raise _build_missing_key_error("encrypt or decrypt")

    try:
        from cryptography.fernet import Fernet
    except ImportError as exc:
        raise RuntimeError(
            "The 'cryptography' package is required for mail credential encryption"
        ) from exc

    from cryptography.fernet import MultiFernet

    try:
        keys = [Fernet(key.encode("utf-8"))]
    except Exception as exc:
        raise RuntimeError(
            f"{MAIL_CREDENTIALS_KEY_ENV} is invalid -  expected a Fernet key"
        ) from exc
    for old_key in (os.getenv(MAIL_CREDENTIALS_OLD_KEYS_ENV) or "").split(","):
        old_key = old_key.strip()
        if not old_key:
            continue
        try:
            keys.append(Fernet(old_key.encode("utf-8")))
        except Exception as exc:
            raise RuntimeError(
                f"{MAIL_CREDENTIALS_OLD_KEYS_ENV} contains an invalid Fernet key"
            ) from exc
    # Encrypts with the first (current) key and decrypts with any of them
    return MultiFernet(keys)


def encrypt_mail_secret(value, field_name="mail credential"):
    if value in (None, ""):
        return value
    if is_encrypted_mail_secret(value):
        return value

    token = _get_fernet().encrypt(str(value).encode("utf-8")).decode("utf-8")
    return f"{MAIL_CREDENTIALS_PREFIX}{token}"


def decrypt_mail_secret(value, field_name="mail credential"):
    if value in (None, ""):
        return value
    if not is_encrypted_mail_secret(value):
        if field_name not in _plaintext_warned:
            _plaintext_warned.add(field_name)
            log.warning(
                "%s is stored in PLAINTEXT in the database - run scripts/migrate_mail_credentials.py to encrypt it",
                field_name,
            )
        return value

    token = value[len(MAIL_CREDENTIALS_PREFIX) :]
    try:
        from cryptography.fernet import InvalidToken
    except ImportError as exc:
        raise RuntimeError(
            "The 'cryptography' package is required for mail credential encryption"
        ) from exc

    try:
        return _get_fernet().decrypt(token.encode("utf-8")).decode("utf-8")
    except InvalidToken as exc:
        raise RuntimeError(
            f"Unable to decrypt {field_name}; check {MAIL_CREDENTIALS_KEY_ENV}"
        ) from exc


def rotate_mail_secret(value, field_name="mail credential"):
    """Re-encrypt a stored value with the current key (plaintext gets encrypted)."""
    if value in (None, ""):
        return value
    if not is_encrypted_mail_secret(value):
        return encrypt_mail_secret(value, field_name=field_name)

    from cryptography.fernet import InvalidToken

    token = value[len(MAIL_CREDENTIALS_PREFIX) :]
    try:
        rotated = _get_fernet().rotate(token.encode("utf-8")).decode("utf-8")
    except InvalidToken as exc:
        raise RuntimeError(
            f"Unable to decrypt {field_name} - is the old key in {MAIL_CREDENTIALS_OLD_KEYS_ENV}?"
        ) from exc
    return f"{MAIL_CREDENTIALS_PREFIX}{rotated}"
