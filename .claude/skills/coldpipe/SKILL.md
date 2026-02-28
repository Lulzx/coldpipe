---
name: coldpipe
description: |
  Run Coldpipe lead generation and outreach workflows using the coldpipe MCP tools.
  Use for: finding leads, managing campaigns, sending emails, checking replies, managing deals.
  Examples: "find dental clinics in Austin", "morning standup", "check replies", "campaign stats".
argument-hint: "[task description, e.g. find dentists in Seattle and start a campaign]"
---

You are operating Coldpipe — a cold outreach pipeline — using MCP tools.
The user's request: $ARGUMENTS

## Start here

Call `get_pending_work` first unless the request is very specific (e.g. "show me leads").

## Available Tools (29 total)

### Orchestration
| Tool | Purpose |
|------|---------|
| `get_pending_work` | **Start every session here.** Returns send queue, enrichment needed, validation needed, mailboxes to check |
| `triage_reply` | Classify a reply using Claude sampling → classification + action |
| `send_campaign_emails` | Send pending emails for a campaign, respecting daily limits |

### Memory (cross-session)
| Tool | Purpose |
|------|---------|
| `save_note_tool` | Persist context across cron runs. Keys: `campaign.{id}.notes`, `global.last_standup` |
| `get_notes_tool` | Retrieve notes, optionally by key prefix |
| `delete_note_tool` | Delete a note by key |

### Lead Discovery
| Tool | Purpose |
|------|---------|
| `scrape_google_maps` | Scrape businesses from Google Maps |
| `scrape_exa` | Neural search via Exa.ai |
| `enrich_websites` | Crawl websites for emails and contact info |

### Lead Management
| Tool | Purpose |
|------|---------|
| `get_leads_tool` | List leads with filters (email_status, source) |
| `get_lead` | Single lead by ID |
| `search_leads_tool` | Search by name/email/company |
| `save_lead` | Create or update a lead |
| `tag_leads_tool` | Add tag to multiple leads |
| `count_leads_tool` | Count leads with optional status filter |
| `validate_leads` | Validate email addresses via MX + SMTP |

### Campaign Management
| Tool | Purpose |
|------|---------|
| `get_campaigns_tool` | List campaigns (filter by status) |
| `get_campaign` | Single campaign with stats |
| `create_campaign_tool` | Create a new campaign |
| `update_campaign_status_tool` | Set status: draft→active→paused→completed |
| `enroll_leads_in_campaign` | Enroll lead IDs into a campaign |
| `get_sequence_steps_tool` | View email sequence for a campaign |
| `add_sequence_step_tool` | Add a step to the sequence |

### Sending & Replies
| Tool | Purpose |
|------|---------|
| `get_send_queue_tool` | Preview what's ready to send for a campaign |
| `check_replies` | Poll IMAP for new replies |

### Deals
| Tool | Purpose |
|------|---------|
| `get_deals_tool` | List deals (filter by stage) |
| `save_deal` | Create or update a deal |

### Analytics
| Tool | Purpose |
|------|---------|
| `get_dashboard_stats` | Leads + today's activity + deal stats |
| `get_campaign_stats_tool` | Detailed stats for one campaign |
| `get_mcp_activity_tool` | Recent tool call history |

## Resources (read without calling tools)

- `leads://summary` — lead counts by status/source/city
- `campaigns://active` — active campaigns with stats
- `activity://recent` — last 20 tool calls
- `notes://all` — all persistent notes

## Prompts (pre-built workflows)

- `morning_standup` — check pending work and handle everything
- `find_and_engage(city, niche)` — full pipeline from scrape to send
- `review_replies` — triage all new replies intelligently

## Common Workflows

### Morning standup
```
1. get_pending_work
2. send_campaign_emails (for each campaign with queue > 0)
3. check_replies (for each IMAP mailbox ID)
4. triage_reply (for each new reply body)
5. save_note_tool key="global.last_standup" value="[summary of actions]"
```

### Find and engage (city + niche)
```
1. scrape_google_maps city="Austin" query="dental clinic"
2. enrich_websites
3. validate_leads
4. create_campaign_tool (or find existing via get_campaigns_tool)
5. get_leads_tool email_status="valid"
6. enroll_leads_in_campaign
7. update_campaign_status_tool status="active"
8. send_campaign_emails
```

### Triage replies
```
1. check_replies mailbox_id=1
2. For each reply: triage_reply body="..." lead_json="{...}"
3. "interested" → save_deal stage="interested"
4. "unsubscribe" → tag_leads_tool tag="unsubscribed"
5. "question" → save_note_tool with draft response
```

## Deal Stages
`lead` → `contacted` → `replied` → `interested` → `meeting_booked` → `proposal_sent` → `closed_won` / `closed_lost`

## Key Notes
- Email openers use `ctx.sample()` under Claude Code — no API key needed
- `send_campaign_emails` respects warmup day limits automatically
- All tool calls are logged to `mcp_activity` — visible at `/activity`
- Use `save_note_tool` to persist decisions across cron runs
- Cron setup: `*/15 * * * * claude -p "Run morning_standup prompt"`
