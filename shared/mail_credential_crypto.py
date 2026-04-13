import os


MAIL_CREDENTIALS_KEY_ENV = "MAIL_CREDENTIALS_KEY"
MAIL_CREDENTIALS_PREFIX = "enc:v1:"


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

    try:
        return Fernet(key.encode("utf-8"))
    except Exception as exc:
        raise RuntimeError(
            f"{MAIL_CREDENTIALS_KEY_ENV} is invalid; expected a Fernet key"
        ) from exc


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
