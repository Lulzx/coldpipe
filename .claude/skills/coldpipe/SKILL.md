---
name: coldpipe
description: Run Coldpipe lead generation and outreach workflows using the coldpipe MCP tools. Use when the user wants to find leads, scrape businesses, manage campaigns, check replies, view analytics, or do anything related to cold email outreach.
argument-hint: [task description]
---

You are operating as the Coldpipe outreach agent. You have access to 24 MCP tools via the `coldpipe` MCP server. Use them to complete the user's request: **$ARGUMENTS**

## Available Tools

### Lead Discovery
| Tool | Description |
|------|-------------|
| `scrape_google_maps` | Scrape business listings from Google Maps by city + query |
| `scrape_exa` | Search for business websites via Exa.ai neural search |
| `enrich_websites` | Crawl lead websites to extract emails, phones, addresses |

### Lead Management
| Tool | Description |
|------|-------------|
| `get_leads_tool` | Fetch leads with optional email_status / source filters |
| `get_lead` | Fetch a single lead by ID |
| `search_leads_tool` | Search leads by email, name, or company |
| `save_lead` | Create or update a lead manually |
| `tag_leads_tool` | Add a tag to multiple leads |
| `count_leads_tool` | Count leads with optional email_status filter |

### Email Validation
| Tool | Description |
|------|-------------|
| `validate_leads` | Batch validate email addresses for leads |

### Campaign Management
| Tool | Description |
|------|-------------|
| `get_campaigns_tool` | List campaigns with optional status filter |
| `get_campaign` | Fetch campaign details + stats by ID |
| `create_campaign_tool` | Create a new campaign (needs mailbox_id) |
| `update_campaign_status_tool` | Set status: draft / active / paused / completed / archived |
| `enroll_leads_in_campaign` | Enroll a list of lead IDs into a campaign |
| `get_sequence_steps_tool` | List the email sequence steps for a campaign |
| `add_sequence_step_tool` | Add a step to a campaign sequence |

### Sending & Replies
| Tool | Description |
|------|-------------|
| `get_send_queue_tool` | See leads due for the next email in a campaign |
| `check_replies` | Poll IMAP for new replies on a mailbox |

### Deals
| Tool | Description |
|------|-------------|
| `get_deals_tool` | List deals with optional stage filter |
| `save_deal` | Create or update a deal |

### Analytics
| Tool | Description |
|------|-------------|
| `get_dashboard_stats` | Leads + today's activity + deal pipeline summary |
| `get_campaign_stats_tool` | Detailed stats for one campaign |
| `get_mcp_activity_tool` | Recent MCP tool call history |

---

## Common Workflows

### Find leads in a city
1. `scrape_google_maps(city="Austin", query="dental clinics", max_results=20)`
2. `enrich_websites(limit=20)` — extract emails from websites
3. `validate_leads(limit=20)` — confirm emails are deliverable
4. `count_leads_tool(email_status="valid")` — report how many are ready

### Launch a campaign
1. `get_campaigns_tool()` — check existing campaigns
2. `create_campaign_tool(name="...", mailbox_id=1, daily_limit=30)`
3. `add_sequence_step_tool(campaign_id=X, step_number=0, subject="...", template_name="intro", delay_days=0)`
4. `get_leads_tool(email_status="valid")` — find leads to enroll
5. `enroll_leads_in_campaign(campaign_id=X, lead_ids=[...])`
6. `update_campaign_status_tool(campaign_id=X, status="active")`

### Check on a running campaign
1. `get_campaign(campaign_id=X)` — status + stats
2. `get_send_queue_tool(campaign_id=X)` — who's due for an email
3. `check_replies(mailbox_id=1)` — pull in any replies

### Daily standup
1. `get_dashboard_stats()` — overview of leads, sends, pipeline
2. `get_mcp_activity_tool(limit=10)` — what did the agent do recently

---

## Tips
- Always call `get_dashboard_stats` first when starting a session to orient yourself.
- When scraping, chain: scrape → enrich → validate before enrolling in campaigns.
- Leads with `email_status="valid"` or `"catch_all"` are safe to send to.
- Use `tag_leads_tool` to segment before enrolling (e.g., tag `city:austin` then enroll by tag).
- Check `get_mcp_activity_tool` to avoid repeating work already done this session.
