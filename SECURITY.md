# Security policy

## Supported versions

Only the latest release in this repository receives fixes.

| Version | Supported |
|---|---|
| 1.2.0 | Yes |
| Older | No |

## Reporting a vulnerability

Please do not report security issues in public issues or discussions.

Report them privately through GitHub: open the **Security** tab of this repository and select **Report a vulnerability**.

Include:

- a description of the issue and its impact
- steps to reproduce, ideally on a development setup
- the Mailing List Manager version, Python version and PostgreSQL version you tested on
- how the admin UI is exposed (reverse proxy, host port forwarding or direct)

Do not include credentials, `.env` contents, real member email addresses or details of a production deployment.

You will get an answer as soon as possible. If the report is confirmed, a fix is released and the reporter is credited unless they prefer not to be.

## Scope

In scope:

- the admin UI in `list-admin/` (routes, templates, authentication, sessions, access checks)
- the mailer in `list/`
- the credential encryption in `shared/` and the migration script in `scripts/`
- the process scripts `list/run_mlist.sh`, `list-admin/ensure-list-admin.sh` and `list-admin/ecosystem.config.js`

Out of scope:

- vulnerabilities in third-party software such as Python, Flask, Werkzeug, PostgreSQL, PM2, Apache or Nginx. Report those to the respective projects.
- the configuration of your own host, web server, TLS or mail servers
- issues that require an `admin` user in the admin UI, unless they let that user do more than the admin UI already allows (for example run commands on the server or read files outside the app)
- deployments that expose the admin UI without TLS, against the README

## Security design notes

- Secrets (`DB_PASS`, `FLASK_SECRET`, `MAIL_CREDENTIALS_KEY`) are read from `.env` files, which are excluded from Git.
- Admin UI passwords are stored as Werkzeug password hashes.
- User, list, configuration and log pages require the `admin` role.
- The session cookie is set with `Secure`, `HttpOnly` and `SameSite=Lax`, so the admin UI needs HTTPS in front of it.
- IMAP and SMTP passwords are stored encrypted (`enc:v1:...`) with the Fernet key in `MAIL_CREDENTIALS_KEY`. Legacy plaintext values stay readable until they are migrated with `scripts/migrate_mail_credentials.py` or saved again through the admin UI.
