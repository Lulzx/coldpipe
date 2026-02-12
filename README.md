# coldpipe

Cold outreach pipeline — scrape, enrich, validate, send, track.

## Features

- **Multi-source scraping** — Google Maps, Yelp, Healthgrades, Zocdoc, Exa.ai, CSV import
- **Website enrichment** — Deep-crawl sites with Crawl4AI + LLM extraction for emails and contact info
- **Pattern-based email discovery** — Generate and score candidate emails from name + domain
- **Email validation** — MX lookup, SMTP verification, catch-all detection, provider identification
- **LLM personalization** — Claude-powered unique openers with template fallback
- **Multi-step sequences** — Configurable email sequences with delay scheduling
- **Mailbox warmup** — Automatic daily send limit progression
- **Reply & bounce tracking** — IMAP polling with automatic sequence management
- **Deal pipeline** — Track leads from contact through close
- **Background daemon** — APScheduler-based automated sending, reply checking, and warmup

## Quick Start

```bash
# Install
uv sync

# Initialize the database
coldpipe db init

# Import leads from CSV
coldpipe scrape import-csv --file data/input/leads.csv

# Enrich leads (scrape websites for emails)
coldpipe enrich run

# Validate email addresses
coldpipe validate run

# Set up a mailbox
coldpipe mailbox add --email you@domain.com --smtp-host smtp.gmail.com \
  --smtp-user you@domain.com --smtp-pass "app-password"

# Create a campaign (auto-generates 4-step sequence)
coldpipe campaign create --name "Q1 Outreach" --mailbox-id 1

# Add leads to the campaign
coldpipe campaign add-leads --campaign-id 1 --email-status valid

# Preview a personalized email
coldpipe send preview --campaign-id 1

# Send emails
coldpipe send run --campaign-id 1

# Or run the daemon for automated processing
coldpipe daemon start
```

## CLI Commands

| Group | Command | Description |
|-------|---------|-------------|
| `db` | `init`, `backup` | Initialize schema, create backups |
| `scrape` | `google-maps`, `yelp`, `healthgrades`, `exa`, `import-csv` | Scrape leads from various sources |
| `leads` | `list`, `search`, `export`, `tag`, `dedupe`, `stats` | Manage and query leads |
| `enrich` | `run`, `status` | Enrich leads by scraping their websites |
| `validate` | `run`, `status` | Validate emails via MX + SMTP |
| `campaign` | `create`, `list`, `add-leads`, `pause`, `resume`, `delete` | Manage outreach campaigns |
| `send` | `preview`, `run`, `run-all`, `status`, `warmup` | Send emails and monitor progress |
| `track` | `check-replies`, `check-bounces`, `stats` | Track replies and bounces |
| `deals` | `list`, `create`, `move`, `close`, `stats` | Manage deal pipeline |
| `mailbox` | `add`, `list`, `test`, `deactivate` | Configure sending mailboxes |
| `daemon` | `start` | Background scheduler for automated processing |

## Architecture

```
coldpipe/
├── cli/              # Typer CLI commands
├── db/               # SQLite schema, migrations, queries (aiosqlite + msgspec)
├── scrapers/         # Google Maps, directories, Exa.ai, website enricher, CSV import
├── mailer/           # Email sender, sequences, templates, personalization, replies, bounces
├── shared/           # Email utils, patterns, scoring, scraping helpers
├── tools/            # Standalone scripts (validate, outreach)
├── config/           # Settings (TOML + env vars), logging
├── tui/              # Textual TUI (optional)
└── data/
    ├── templates/    # Jinja2 email templates
    └── input/        # CSV files for import
```

## Configuration

Create a `coldpipe.toml` in the project root:

```toml
[send]
daily_limit = 30
send_window_start = "08:00"
send_window_end = "17:00"
timezone = "America/New_York"

[llm]
model = "claude-haiku-4-5"
max_concurrent = 5
max_opener_words = 30

[scraper]
max_concurrent = 500
timeout = 5
```

Environment variables:

| Variable | Description |
|----------|-------------|
| `ANTHROPIC_API_KEY` | Claude API key for LLM personalization |
| `EXA_API_KEY` | Exa.ai API key for neural search |
| `DB_PATH` | Override default database path |
| `LOG_LEVEL` | Logging level (default: `INFO`) |

## License

MIT
