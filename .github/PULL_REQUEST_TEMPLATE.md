<!-- Do not include .env contents, credentials, real member email addresses or mail server host names. -->

## What changed and why


## Related issue or discussion

<!-- For example: Fixes #12 -->

## How I tested it

- Python version:
- PostgreSQL version:
- Tested on: local / staging / shared host
- Browser (for UI changes):

## Checklist

- [ ] `list-admin/sql/schema.sql` matches the code (tables, columns, unique indexes and `ON CONFLICT` targets)
- [ ] Migration SQL for existing databases is included, or no schema change was made
- [ ] New environment variables are added to the matching `.env.example` and documented in the README
- [ ] No secrets, `.env` files, database dumps or host-specific values are committed
- [ ] New or changed texts use `t()` and all files in `list-admin/i18n/` contain the same keys
- [ ] Admin-only routes keep the `@requires_admin` decorator
- [ ] Tested on a development or staging setup, not production
- [ ] Screenshots added for UI changes (optional)
