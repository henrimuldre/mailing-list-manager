# Contributing

Thanks for helping improve Mailing List Manager. Bug reports, ideas, translations and code changes are all welcome.

## Before you start

- **Questions and ideas:** start a discussion in [Discussions](../../discussions).
- **Bugs:** open an [issue](../../issues/new/choose) with the bug report template.
- **Security issues:** do not open a public issue. Follow the [security policy](SECURITY.md).
- **Larger changes:** open an issue or discussion first, so we can agree on the approach before you spend time on it.

Never include `.env` contents, database dumps, credentials, `MAIL_CREDENTIALS_KEY`, `FLASK_SECRET`, real member email addresses, mail server host names or other sensitive details in issues, screenshots, logs or commits.

## Development setup

Follow [Project Setup](README.md#project-setup) in the README, with these differences:

- Use a separate PostgreSQL database for development. Do not develop against a production database.
- Use test mailboxes for IMAP and SMTP. Do not point a development list at a real list address or real members.
- Run the admin UI directly instead of through PM2, for example:

```bash
cd list-admin
.venv/bin/python app.py --host 127.0.0.1 --port 9010
```

## Rules for code changes

- **Keep the schema and the code in sync.** If you change a table, column, constraint or index, update [`list-admin/sql/schema.sql`](list-admin/sql/schema.sql) in the same pull request. `ON CONFLICT` targets in the code must match an existing unique index or constraint.
- **Existing deployments need an upgrade path.** If a schema change affects an existing database, include the SQL to migrate it (in the pull request description or as a script in `scripts/`) and make it safe to run more than once where possible.
- **No secrets in the repository.** Configuration goes through environment variables. If you add a variable, add it with a placeholder value to the matching `.env.example` and document it in the README.
- **No host-specific values.** No hard-coded host names, paths, ports, user names, list addresses or customer-specific logic.
- **Mail credentials stay encrypted.** Read and write IMAP/SMTP passwords through [`shared/mail_credential_crypto.py`](shared/mail_credential_crypto.py). Do not log them or show them in the UI.
- **Keep the access checks.** Pages and actions for users, lists, configuration and logs must keep the `@requires_admin` decorator. New admin-only routes need it as well.
- **Texts go through `t()`.** Every user-facing text in the admin UI must use a translation key. If you add, change or remove a key, update all files in [`list-admin/i18n/`](list-admin/i18n/) (`en.json`, `et.json`, `fi.json`) so they contain the same keys.
- **Shared-hosting friendly.** The app must keep working on a shared host without root access: no system packages beyond the documented requirements, no services that need root and no build step.

## Translations

To add a language, copy `list-admin/i18n/en.json` to `<language-code>.json` and translate the values. Keep the keys unchanged and keep every `{placeholder}` from the English text.

## Testing

There are no automated tests yet. Before you open a pull request, test on a development or staging setup:

1. The admin UI starts without errors and you can log in and log out.
2. Pages you changed load in both light and dark theme and in every language without missing translation keys.
3. Creating, editing and deleting lists and members still works, including duplicate member addresses.
4. If you modified the mailer, run `list/run_mlist.sh` against a test list and check that a test message is fetched, redistributed and recorded on the Messages page and that `list/logs/mlist.log` has no errors.
5. If you modified the schema, apply `schema.sql` to an empty database and run your migration SQL against a copy of an existing database.
6. If you modified `ensure-list-admin.sh` or `ecosystem.config.js`, test starting, restarting and recovering the admin UI through PM2.

## Pull requests

- One topic per pull request.
- Describe what changed and why and how you tested it.
- Include screenshots for UI changes, with sensitive details removed.

By contributing, you agree that your contribution is licensed under the [GNU General Public License v3.0](LICENSE).
